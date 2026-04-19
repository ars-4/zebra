import json
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from .models import (
    CommandServerCreate, RestServerCreate,
    GithubServerCreate, SlackServerCreate, GmailServerCreate,
    OllamaChatRequest,
)
from .db import (
    get_db, db_list_servers, db_get_server, db_get_server_by_name,
    db_create_server, db_update_server, # will use this stupid sometime else
    db_delete_server,
)
from .utils import (
    sse_event, sse_error, # Some stuff only want you see dead
    stream_command_server, stream_rest_server,
    github_call, slack_call, gmail_call,
    ollama_list_models, ollama_chat_stream, ollama_chat_once,
    kill_command_server, list_running_servers,
    OLLAMA_BASE,
)

router = APIRouter()


def _sse_response(generator):
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _get_server_or_404(db, server_id: int) -> dict:
    server = await db_get_server(db, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")
    return server


async def _get_server_by_name_or_404(db, name: str) -> dict:
    server = await db_get_server_by_name(db, name)
    if not server:
        raise HTTPException(status_code=404, detail=f"Server '{name}' not found")
    return server


@router.get("/mcp-servers", tags=["CRUD"])
async def list_servers(db=Depends(get_db)):
    servers = await db_list_servers(db)
    running = list_running_servers()
    for s in servers:
        s["is_running"] = s["name"] in running
    return servers


@router.get("/mcp-servers/{server_id}", tags=["CRUD"])
async def get_server(server_id: int, db=Depends(get_db)):
    return await _get_server_or_404(db, server_id)


@router.delete("/mcp-servers/{server_id}", tags=["CRUD"])
async def delete_server(server_id: int, db=Depends(get_db)):
    server = await _get_server_or_404(db, server_id)
    await kill_command_server(server["name"])
    deleted = await db_delete_server(db, server_id)
    if not deleted:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"detail": "deleted"}


@router.post("/mcp-servers/command", tags=["Create"])
async def create_command_server(body: CommandServerCreate, db=Depends(get_db)):
    config = {"command": body.command, "args": body.args, "env": body.env, "idle_timeout": body.idle_timeout}
    return await db_create_server(db, name=body.name, description=body.description,
                                   server_type="command", config=config,
                                   auto_start=body.auto_start, idle_timeout=body.idle_timeout)


@router.post("/mcp-servers/rest-api", tags=["Create"])
async def create_rest_server(body: RestServerCreate, db=Depends(get_db)):
    config = {"host": body.host, "headers": body.headers,
              "endpoints": [e.dict() for e in body.endpoints], "idle_timeout": body.idle_timeout}
    return await db_create_server(db, name=body.name, description=body.description,
                                   server_type="rest_api", config=config,
                                   auto_start=body.auto_start, idle_timeout=body.idle_timeout)


@router.post("/mcp-servers/github", tags=["Create"])
async def create_github_server(body: GithubServerCreate, db=Depends(get_db)):
    return await db_create_server(db, name=body.name, description=body.description,
                                   server_type="github",
                                   config={"personal_access_token": body.personal_access_token},
                                   auto_start=body.auto_start, idle_timeout=body.idle_timeout)


@router.post("/mcp-servers/slack", tags=["Create"])
async def create_slack_server(body: SlackServerCreate, db=Depends(get_db)):
    return await db_create_server(db, name=body.name, description=body.description,
                                   server_type="slack", config={"bot_token": body.bot_token},
                                   auto_start=body.auto_start, idle_timeout=body.idle_timeout)


@router.post("/mcp-servers/gmail", tags=["Create"])
async def create_gmail_server(body: GmailServerCreate, db=Depends(get_db)):
    config = {"client_id": body.client_id, "client_secret": body.client_secret,
              "refresh_token": body.refresh_token}
    return await db_create_server(db, name=body.name, description=body.description,
                                   server_type="gmail", config=config,
                                   auto_start=body.auto_start, idle_timeout=body.idle_timeout)


@router.post("/mcp_servers/{name}/mcp", tags=["MCP SSE"])
async def mcp_endpoint(name: str, request: Request, db=Depends(get_db)):
    server = await _get_server_by_name_or_404(db, name)
    config = server["config"]
    stype  = server["server_type"]
    try:
        body = await request.json()
    except Exception:
        body = {}

    if stype == "command":
        return _sse_response(stream_command_server(name, config, body.get("payload", body)))
    if stype == "rest_api":
        return _sse_response(stream_rest_server(config, body.get("endpoint", "/"),
                                                body.get("method", "GET"),
                                                body.get("body"), body.get("params")))
    if stype == "github":
        return _sse_response(github_call(config, body.get("action", "list_repos"), body.get("params", {})))
    if stype == "slack":
        return _sse_response(slack_call(config, body.get("action", "list_channels"), body.get("params", {})))
    if stype == "gmail":
        return _sse_response(gmail_call(config, body.get("action", "list_messages"), body.get("params", {})))

    raise HTTPException(status_code=400, detail=f"Unknown server type: {stype}")


@router.post("/mcp_servers/{name}/call", tags=["MCP SSE"])
async def mcp_call(name: str, request: Request, db=Depends(get_db)):
    """
    Like /mcp but collects all SSE events and returns a single JSON result.
    The frontend uses this to get tool results before sending them back to Ollama.
    """
    server = await _get_server_by_name_or_404(db, name)
    config = server["config"]
    stype  = server["server_type"]
    try:
        body = await request.json()
    except Exception:
        body = {}

    results = []

    async def collect(gen):
        async for chunk in gen:
            for line in chunk.split("\n"):
                if line.startswith("data:"):
                    try:
                        results.append(json.loads(line[5:].strip()))
                    except Exception:
                        pass

    if stype == "command":
        await collect(stream_command_server(name, config, body.get("payload", body)))
    elif stype == "rest_api":
        await collect(stream_rest_server(config, body.get("endpoint", "/"),
                                         body.get("method", "GET"),
                                         body.get("body"), body.get("params")))
    elif stype == "github":
        await collect(github_call(config, body.get("action", "list_repos"), body.get("params", {})))
    elif stype == "slack":
        await collect(slack_call(config, body.get("action", "list_channels"), body.get("params", {})))
    elif stype == "gmail":
        await collect(gmail_call(config, body.get("action", "list_messages"), body.get("params", {})))
    else:
        raise HTTPException(status_code=400, detail=f"Unknown server type: {stype}")

    data_results = [r for r in results if r.get("status") != "done"]
    return {"results": data_results, "tool": name, "server_type": stype}



@router.delete("/mcp_servers/{name}/kill", tags=["Process"])
async def kill_server_process(name: str, db=Depends(get_db)):
    await _get_server_by_name_or_404(db, name)
    await kill_command_server(name)
    return {"detail": f"Process '{name}' killed (if it was running)"}


@router.get("/mcp_servers/running", tags=["Process"])
async def get_running_servers():
    return {"running": list_running_servers()}


@router.get("/ollama/models", tags=["Ollama"])
async def get_ollama_models():
    try:
        models = await ollama_list_models()
        return {"models": models}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Ollama unreachable: {exc}")

@router.post("/ollama/chat", tags=["Ollama"])
async def ollama_chat(body: OllamaChatRequest, db=Depends(get_db)):
    messages = [{"role": m.role, "content": m.content} for m in body.chat_history]
    messages.append({"role": "user", "content": body.chat_message})

    system: str | None = None
    if body.tool:
        server = await db_get_server_by_name(db, body.tool)
        if not server:
            raise HTTPException(status_code=404, detail=f"Tool '{body.tool}' not found")

        stype = server["server_type"]

        action_docs = {
            "github": (
                "Actions: list_repos, get_repo {owner,repo}, list_issues {owner,repo}, "
                "get_issue {owner,repo,number}, list_prs {owner,repo}, "
                "get_pr {owner,repo,number}, list_branches {owner,repo}, list_commits {owner,repo}."
            ),
            "slack": (
                "Actions: list_channels, post_message {channel,text}, "
                "get_messages {channel}, get_user {user}, list_users."
            ),
            "gmail": (
                "Actions: list_messages {q?,maxResults?}, get_message {message_id}, "
                "send_message {to,subject,body}, list_labels, get_profile."
            ),
            "rest_api": "Actions: use endpoint path + method + params as needed.",
            "command": "Actions: send a payload dict to the command process.",
        }

        system = f"""You are an assistant with access to an MCP tool called '{server["name"]}' (type: {stype}).

{action_docs.get(stype, "")}

IMPORTANT RULES:
- If the user's request requires using the tool, respond with ONLY a JSON object (no prose, no markdown fences) in this exact format:
  {{"tool_call": true, "action": "<action_name>", "params": {{...}}}}
- If the request does NOT require the tool, or if you have already received tool results in the conversation and just need to present them, respond normally in plain text or markdown.
- Never make up data. If you need fresh data from the tool, emit the tool_call JSON.
- Repository names should be passed exactly as given, e.g. "owner/repo" → owner="owner", repo="repo"."""

    if body.stream:
        return _sse_response(ollama_chat_stream(body.model, messages, system))
    else:
        try:
            result = await ollama_chat_once(body.model, messages, system)
            return result
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc))