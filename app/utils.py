"""
utils.py
────────
All helper logic lives here so views.py stays clean:
  - Process registry (Type 1 – command servers with idle timeout)
  - SSE event formatter
  - MCP tool call dispatcher (Type 1, 2, 3)
  - Ollama client helpers
"""

import asyncio
import json
import time
import httpx
from typing import AsyncGenerator, Optional


# ──────────────────────────────────────────────
# SSE helpers
# ──────────────────────────────────────────────

def sse_event(data: dict | str, event: str = "message") -> str:
    """Format a single SSE event string."""
    payload = data if isinstance(data, str) else json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"


async def sse_error(message: str) -> AsyncGenerator[str, None]:
    yield sse_event({"error": message}, event="error")


# ──────────────────────────────────────────────
# Type 1 – Process registry with idle timeout
# ──────────────────────────────────────────────

class _ProcessEntry:
    def __init__(self, proc: asyncio.subprocess.Process, idle_timeout: int):
        self.proc = proc
        self.idle_timeout = idle_timeout
        self.last_used: float = time.monotonic()
        self._watchdog: Optional[asyncio.Task] = None

    def touch(self):
        self.last_used = time.monotonic()


# name -> _ProcessEntry
_process_registry: dict[str, _ProcessEntry] = {}


async def _idle_watchdog(name: str, entry: _ProcessEntry):
    """Kill a process after it has been idle for entry.idle_timeout seconds."""
    while True:
        await asyncio.sleep(10)
        idle_for = time.monotonic() - entry.last_used
        if idle_for >= entry.idle_timeout:
            await kill_command_server(name)
            break


async def get_or_start_process(name: str, command: str, args: list[str],
                                env: dict[str, str], idle_timeout: int) -> _ProcessEntry:
    """Return a running process for this server, starting it if needed."""
    if name in _process_registry:
        entry = _process_registry[name]
        if entry.proc.returncode is None:   # still alive
            entry.touch()
            return entry
        else:
            del _process_registry[name]     # stale – restart

    import os
    merged_env = {**os.environ, **env}
    proc = await asyncio.create_subprocess_exec(
        command, *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=merged_env,
    )
    entry = _ProcessEntry(proc, idle_timeout)
    _process_registry[name] = entry
    entry._watchdog = asyncio.create_task(_idle_watchdog(name, entry))
    return entry


async def kill_command_server(name: str):
    entry = _process_registry.pop(name, None)
    if entry and entry.proc.returncode is None:
        entry.proc.terminate()
        try:
            await asyncio.wait_for(entry.proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            entry.proc.kill()
    # cancel watchdog
    if entry and entry._watchdog:
        entry._watchdog.cancel()


def list_running_servers() -> list[str]:
    return [n for n, e in _process_registry.items() if e.proc.returncode is None]


# ──────────────────────────────────────────────
# Type 1 – send a JSON-RPC style message and yield SSE
# ──────────────────────────────────────────────

async def stream_command_server(name: str, config: dict,
                                 payload: dict) -> AsyncGenerator[str, None]:
    """Dispatch to one-shot or keep-alive mode based on config."""
    if config.get("one_shot", True):
        async for chunk in _run_oneshot(config, payload):
            yield chunk
    else:
        async for chunk in _run_keepalive(name, config, payload):
            yield chunk


async def _run_oneshot(config: dict, payload: dict) -> AsyncGenerator[str, None]:
    """Run command once, stream stdout line by line, process exits when done."""
    import os
    merged_env = {**os.environ, **config.get("env", {})}
    args = payload.get("args", config.get("args", []))
    stdin_data = payload.get("stdin")

    try:
        proc = await asyncio.create_subprocess_exec(
            config["command"], *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=merged_env,
        )
        stdout, _ = await asyncio.wait_for(
            proc.communicate(stdin_data.encode() if stdin_data else None),
            timeout=60,
        )
        for raw_line in stdout.decode(errors="replace").splitlines():
            line = raw_line.rstrip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                parsed = {"output": line}
            yield sse_event(parsed)
        yield sse_event({"exit_code": proc.returncode, "status": "done"}, event="done")
    except asyncio.TimeoutError:
        async for chunk in sse_error("Command timed out after 60s"):
            yield chunk
    except Exception as exc:
        async for chunk in sse_error(f"Failed to run command: {exc}"):
            yield chunk


async def _run_keepalive(name: str, config: dict,
                          payload: dict) -> AsyncGenerator[str, None]:
    """Start/reuse a long-running process, write JSON line, read until blank line."""
    try:
        entry = await get_or_start_process(
            name=name,
            command=config["command"],
            args=config.get("args", []),
            env=config.get("env", {}),
            idle_timeout=config.get("idle_timeout", 300),
        )
    except Exception as exc:
        async for chunk in sse_error(f"Failed to start process: {exc}"):
            yield chunk
        return

    try:
        line = json.dumps(payload) + "\n"
        entry.proc.stdin.write(line.encode())
        await entry.proc.stdin.drain()
        entry.touch()

        assert entry.proc.stdout is not None
        async for raw in entry.proc.stdout:
            entry.touch()
            text = raw.decode(errors="replace").rstrip()
            if not text:
                break
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {"output": text}
            yield sse_event(parsed)
        yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


# ──────────────────────────────────────────────
# Type 2 – REST API bridge → SSE
# ──────────────────────────────────────────────

async def stream_rest_server(config: dict, endpoint_path: str,
                              method: str, body: Optional[dict] = None,
                              query: Optional[dict] = None,
                              path_params: Optional[dict] = None) -> AsyncGenerator[str, None]:
    """Call a REST endpoint and stream the response as SSE."""
    base = config["host"].rstrip("/")

    # Substitute path params: /users/{id} + {"id":"42"} → /users/42
    path = endpoint_path
    for k, v in (path_params or {}).items():
        path = path.replace(f"{{{k}}}", str(v))

    url = f"{base}{path}"
    headers = config.get("headers", {})

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=query or {},
                json=body if method.upper() in ("POST", "PUT", "PATCH") else None,
            )
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}

            yield sse_event({"status_code": resp.status_code, "data": data})
            yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


# ──────────────────────────────────────────────
# Type 3 – GitHub
# ──────────────────────────────────────────────

GITHUB_API = "https://api.github.com"


async def github_call(config: dict, action: str,
                       params: dict) -> AsyncGenerator[str, None]:
    """
    Supported actions: list_repos, get_repo, list_issues, get_issue,
                       list_prs, get_pr, list_branches
    params: { owner, repo, number, per_page, page, ... }
    """
    token = config["personal_access_token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    routes = {
        "list_repos":    ("GET", "/user/repos"),
        "get_repo":      ("GET", "/repos/{owner}/{repo}"),
        "list_issues":   ("GET", "/repos/{owner}/{repo}/issues"),
        "get_issue":     ("GET", "/repos/{owner}/{repo}/issues/{number}"),
        "list_prs":      ("GET", "/repos/{owner}/{repo}/pulls"),
        "get_pr":        ("GET", "/repos/{owner}/{repo}/pulls/{number}"),
        "list_branches": ("GET", "/repos/{owner}/{repo}/branches"),
        "list_commits":  ("GET", "/repos/{owner}/{repo}/commits"),
    }

    if action not in routes:
        async for chunk in sse_error(f"Unknown GitHub action '{action}'. "
                                      f"Available: {list(routes)}"):
            yield chunk
        return

    method, path_tpl = routes[action]
    try:
        path = path_tpl.format(**params)
    except KeyError as e:
        async for chunk in sse_error(f"Missing param {e} for action '{action}'"):
            yield chunk
        return

    query = {k: v for k, v in params.items()
             if k not in ("owner", "repo", "number")}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.request(method, GITHUB_API + path,
                                         headers=headers, params=query)
            yield sse_event({"status_code": resp.status_code, "data": resp.json()})
            yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


# ──────────────────────────────────────────────
# Type 3 – Slack
# ──────────────────────────────────────────────

SLACK_API = "https://slack.com/api"


async def slack_call(config: dict, action: str,
                      params: dict) -> AsyncGenerator[str, None]:
    """
    Supported actions: list_channels, post_message, get_messages,
                       get_user, list_users
    params: { channel, text, limit, user, ... }
    """
    token = config["bot_token"]
    headers = {"Authorization": f"Bearer {token}"}

    action_map = {
        "list_channels": ("GET",  "/conversations.list",     {}),
        "post_message":  ("POST", "/chat.postMessage",       {"channel", "text"}),
        "get_messages":  ("GET",  "/conversations.history",  {"channel"}),
        "get_user":      ("GET",  "/users.info",             {"user"}),
        "list_users":    ("GET",  "/users.list",             {}),
    }

    if action not in action_map:
        async for chunk in sse_error(f"Unknown Slack action '{action}'. "
                                      f"Available: {list(action_map)}"):
            yield chunk
        return

    method, path, _ = action_map[action]
    url = SLACK_API + path

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if method == "POST":
                resp = await client.post(url, headers=headers, json=params)
            else:
                resp = await client.get(url, headers=headers, params=params)

            data = resp.json()
            if not data.get("ok"):
                async for chunk in sse_error(data.get("error", "Slack API error")):
                    yield chunk
                return

            yield sse_event({"status_code": resp.status_code, "data": data})
            yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


# ──────────────────────────────────────────────
# Type 3 – Gmail (OAuth2)
# ──────────────────────────────────────────────

GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"


async def _gmail_access_token(config: dict) -> str:
    """Exchange refresh_token for a short-lived access token."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(GMAIL_TOKEN_URL, data={
            "client_id":     config["client_id"],
            "client_secret": config["client_secret"],
            "refresh_token": config["refresh_token"],
            "grant_type":    "refresh_token",
        })
        resp.raise_for_status()
        return resp.json()["access_token"]


async def gmail_call(config: dict, action: str,
                      params: dict) -> AsyncGenerator[str, None]:
    """
    Supported actions: list_messages, get_message, send_message,
                       list_labels, get_profile
    params: { message_id, query, max_results, to, subject, body, ... }
    """
    action_map = {
        "list_messages": ("GET",  "/messages"),
        "get_message":   ("GET",  "/messages/{message_id}"),
        "send_message":  ("POST", "/messages/send"),
        "list_labels":   ("GET",  "/labels"),
        "get_profile":   ("GET",  ""),          # /users/me
    }

    if action not in action_map:
        async for chunk in sse_error(f"Unknown Gmail action '{action}'. "
                                      f"Available: {list(action_map)}"):
            yield chunk
        return

    try:
        access_token = await _gmail_access_token(config)
    except Exception as exc:
        async for chunk in sse_error(f"Gmail auth failed: {exc}"):
            yield chunk
        return

    headers = {"Authorization": f"Bearer {access_token}"}
    method, path_tpl = action_map[action]

    try:
        path = path_tpl.format(**params)
    except KeyError as e:
        async for chunk in sse_error(f"Missing param {e} for action '{action}'"):
            yield chunk
        return

    url = GMAIL_API + path
    body = None

    # Build RFC-2822 raw message for send_message
    if action == "send_message":
        import base64, email.mime.text
        msg = email.mime.text.MIMEText(params.get("body", ""))
        msg["To"] = params.get("to", "")
        msg["Subject"] = params.get("subject", "")
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        body = {"raw": raw}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if method == "POST":
                resp = await client.post(url, headers=headers, json=body)
            else:
                query = {k: v for k, v in params.items() if k != "message_id"}
                resp = await client.get(url, headers=headers, params=query)

            yield sse_event({"status_code": resp.status_code, "data": resp.json()})
            yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


# ──────────────────────────────────────────────
# Ollama helpers
# ──────────────────────────────────────────────

OLLAMA_BASE = "http://localhost:11434"


async def ollama_list_models() -> list[str]:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{OLLAMA_BASE}/api/tags")
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]


async def ollama_chat_stream(model: str, messages: list[dict],
                              tool_context: Optional[str] = None
                              ) -> AsyncGenerator[str, None]:
    """Stream Ollama chat response as SSE. Optionally injects tool_context."""
    if tool_context:
        messages = [{"role": "system", "content": tool_context}] + messages

    payload = {"model": model, "messages": messages, "stream": True}

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream("POST", f"{OLLAMA_BASE}/api/chat",
                                      json=payload) as resp:
                async for raw in resp.aiter_lines():
                    if not raw.strip():
                        continue
                    try:
                        chunk = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    yield sse_event(chunk)
                    if chunk.get("done"):
                        break
        yield sse_event({"status": "done"}, event="done")
    except Exception as exc:
        async for chunk in sse_error(str(exc)):
            yield chunk


async def ollama_chat_once(model: str, messages: list[dict],
                            tool_context: Optional[str] = None) -> dict:
    """Non-streaming Ollama call. Returns the full message dict."""
    if tool_context:
        messages = [{"role": "system", "content": tool_context}] + messages

    payload = {"model": model, "messages": messages, "stream": False}
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(f"{OLLAMA_BASE}/api/chat", json=payload)
        resp.raise_for_status()
        return resp.json()
