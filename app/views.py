import json
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from .models import (
    CommandServerCreate, RestServerCreate,
    GithubServerCreate, SlackServerCreate, GmailServerCreate,
    OllamaChatRequest,
)
from .db import (
    get_db, db_list_servers, db_get_server, db_get_server_by_name,
    db_create_server, db_delete_server,
    db_update_server, # Going to use it later
)
from .utils import (
    sse_event, sse_error, # some shit is hard, use it and you'll see yourself breaking up
    stream_command_server, stream_rest_server,
    github_call, slack_call, gmail_call,
    ollama_list_models, ollama_chat_stream, ollama_chat_once,
    kill_command_server, list_running_servers,
)

router = APIRouter()


def _sse_response(generator):
    return StreamingResponse(generator,
                             media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


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
    config = {
        "command":      body.command,
        "args":         body.args,
        "env":          body.env,
        "idle_timeout": body.idle_timeout,
    }
    record = await db_create_server(
        db,
        name=body.name,
        description=body.description,
        server_type="command",
        config=config,
        auto_start=body.auto_start,
        idle_timeout=body.idle_timeout,
    )
    return record


@router.post("/mcp-servers/rest-api", tags=["Create"])
async def create_rest_server(body: RestServerCreate, db=Depends(get_db)):
    config = {
        "host":      body.host,
        "headers":   body.headers,
        "endpoints": [e.dict() for e in body.endpoints],
        "idle_timeout": body.idle_timeout,
    }
    record = await db_create_server(
        db,
        name=body.name,
        description=body.description,
        server_type="rest_api",
        config=config,
        auto_start=body.auto_start,
        idle_timeout=body.idle_timeout,
    )
    return record


@router.post("/mcp-servers/github", tags=["Create"])
async def create_github_server(body: GithubServerCreate, db=Depends(get_db)):
    config = {"personal_access_token": body.personal_access_token}
    record = await db_create_server(
        db,
        name=body.name,
        description=body.description,
        server_type="github",
        config=config,
        auto_start=body.auto_start,
        idle_timeout=body.idle_timeout,
    )
    return record


@router.post("/mcp-servers/slack", tags=["Create"])
async def create_slack_server(body: SlackServerCreate, db=Depends(get_db)):
    config = {"bot_token": body.bot_token}
    record = await db_create_server(
        db,
        name=body.name,
        description=body.description,
        server_type="slack",
        config=config,
        auto_start=body.auto_start,
        idle_timeout=body.idle_timeout,
    )
    return record


@router.post("/mcp-servers/gmail", tags=["Create"])
async def create_gmail_server(body: GmailServerCreate, db=Depends(get_db)):
    config = {
        "client_id":     body.client_id,
        "client_secret": body.client_secret,
        "refresh_token": body.refresh_token,
    }
    record = await db_create_server(
        db,
        name=body.name,
        description=body.description,
        server_type="gmail",
        config=config,
        auto_start=body.auto_start,
        idle_timeout=body.idle_timeout,
    )
    return record



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
        payload = body.get("payload", body)
        return _sse_response(stream_command_server(name, config, payload))

    if stype == "rest_api":
        endpoint = body.get("endpoint", "/")
        method   = body.get("method", "GET")
        params   = body.get("params")
        data     = body.get("body")
        return _sse_response(stream_rest_server(config, endpoint, method, data, params))

    if stype == "github":
        action = body.get("action", "list_repos")
        params = body.get("params", {})
        return _sse_response(github_call(config, action, params))

    if stype == "slack":
        action = body.get("action", "list_channels")
        params = body.get("params", {})
        return _sse_response(slack_call(config, action, params))

    if stype == "gmail":
        action = body.get("action", "list_messages")
        params = body.get("params", {})
        return _sse_response(gmail_call(config, action, params))

    raise HTTPException(status_code=400, detail=f"Unknown server type: {stype}")


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
        raise HTTPException(status_code=503,
                            detail=f"Ollama unreachable: {exc}")


@router.post("/ollama/chat", tags=["Ollama"])
async def ollama_chat(body: OllamaChatRequest, db=Depends(get_db)):
    messages = [{"role": m.role, "content": m.content}
                for m in body.chat_history]
    messages.append({"role": "user", "content": body.chat_message})
    tool_context: str | None = None
    if body.tool:
        server = await db_get_server_by_name(db, body.tool)
        if server:
            tool_context = (
                f"You have access to an MCP tool called '{server['name']}'.\n"
                f"Description: {server.get('description') or 'No description'}\n"
                f"Type: {server['server_type']}\n"
                f"When the user asks you to use it, call the appropriate action "
                f"and present the result clearly."
            )
        else:
            raise HTTPException(status_code=404,
                                detail=f"Tool '{body.tool}' not found")

    if body.stream:
        return _sse_response(
            ollama_chat_stream(body.model, messages, tool_context)
        )
    else:
        try:
            result = await ollama_chat_once(body.model, messages, tool_context)
            return result
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc))