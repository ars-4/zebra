from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .db import init_db, get_db, db_list_servers
from .utils import get_or_start_process
from .views import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    async for db in get_db():
        servers = await db_list_servers(db)
        for s in servers:
            if s.get("auto_start") and s["server_type"] == "command":
                cfg = s["config"]
                try:
                    await get_or_start_process(
                        name=s["name"],
                        command=cfg["command"],
                        args=cfg.get("args", []),
                        env=cfg.get("env", {}),
                        idle_timeout=cfg.get("idle_timeout", 300),
                    )
                    print(f"[auto_start] Started '{s['name']}'")
                except Exception as exc:
                    print(f"[auto_start] Failed to start '{s['name']}': {exc}")

    yield 


app = FastAPI(
    title="MCP Gateway",
    description=(
        "A single backend that converts command processes, REST APIs, "
        "and pre-configured services into "
        "SSE-based MCP servers, with Ollama chat integration."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)