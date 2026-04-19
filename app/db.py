import json
import aiosqlite
from datetime import datetime
from typing import Optional
from .models import MCPServer, ServerType

DB_PATH = "mcp_gateway.db"


# Schema
CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS mcp_servers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    server_type TEXT NOT NULL,
    config      TEXT NOT NULL,          -- JSON blob of type-specific config
    auto_start  INTEGER DEFAULT 0,
    idle_timeout INTEGER DEFAULT 300,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES)
        await db.commit()


# Dependency
async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


# Helpers
def _row_to_server(row: aiosqlite.Row) -> dict:
    d = dict(row)
    d["config"] = json.loads(d["config"])
    d["auto_start"] = bool(d["auto_start"])
    return d


# CRUD
async def db_list_servers(db) -> list[dict]:
    cursor = await db.execute("SELECT * FROM mcp_servers ORDER BY id")
    rows = await cursor.fetchall()
    return [_row_to_server(r) for r in rows]


async def db_get_server(db, server_id: int) -> Optional[dict]:
    cursor = await db.execute("SELECT * FROM mcp_servers WHERE id = ?", (server_id,))
    row = await cursor.fetchone()
    return _row_to_server(row) if row else None


async def db_get_server_by_name(db, name: str) -> Optional[dict]:
    cursor = await db.execute("SELECT * FROM mcp_servers WHERE name = ?", (name,))
    row = await cursor.fetchone()
    return _row_to_server(row) if row else None


async def db_create_server(db, name: str, description: Optional[str],
                            server_type: str, config: dict,
                            auto_start: bool, idle_timeout: int) -> dict:
    now = datetime.utcnow().isoformat()
    cursor = await db.execute(
        """INSERT INTO mcp_servers
           (name, description, server_type, config, auto_start, idle_timeout, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (name, description, server_type, json.dumps(config),
         int(auto_start), idle_timeout, now, now)
    )
    await db.commit()
    return await db_get_server(db, cursor.lastrowid)


async def db_update_server(db, server_id: int, updates: dict) -> Optional[dict]:
    updates["updated_at"] = datetime.utcnow().isoformat()
    if "config" in updates:
        updates["config"] = json.dumps(updates["config"])
    if "auto_start" in updates:
        updates["auto_start"] = int(updates["auto_start"])

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [server_id]
    await db.execute(f"UPDATE mcp_servers SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await db_get_server(db, server_id)


async def db_delete_server(db, server_id: int) -> bool:
    result = await db.execute("DELETE FROM mcp_servers WHERE id = ?", (server_id,))
    await db.commit()
    return result.rowcount > 0