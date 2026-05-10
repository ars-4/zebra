from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime
from enum import Enum


# ──────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────

class ServerType(str, Enum):
    command = "command"    # Type 1
    rest_api = "rest_api"  # Type 2
    github = "github"      # Type 3
    slack = "slack"        # Type 3
    gmail = "gmail"        # Type 3


class HttpMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


# ──────────────────────────────────────────────
# Base MCP Server (stored in DB)
# ──────────────────────────────────────────────

class MCPServer(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    server_type: ServerType
    auto_start: bool = False           # if True, restarts on app boot
    idle_timeout: int = 300            # seconds before process is killed (Type 1)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# Type 1 – Command-based MCP server
# ──────────────────────────────────────────────

class CommandServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    command: str                       # e.g. "psql", "node", "curl"
    args: list[str] = []              # e.g. ["-X", "GET", "https://..."]
    env: dict[str, str] = {}          # extra environment variables
    one_shot: bool = True              # True = run, collect output, exit. False = keep-alive
    auto_start: bool = False
    idle_timeout: int = 300


# ──────────────────────────────────────────────
# Type 2 – REST API MCP server
# ──────────────────────────────────────────────

class EndpointParam(BaseModel):
    name: str
    value: Any
    param_type: str = "query"          # "query" | "body" | "path" | "header"


class RestEndpoint(BaseModel):
    path: str                          # e.g. "/users/{id}"
    method: HttpMethod
    description: Optional[str] = None
    params: list[EndpointParam] = []   # query params
    body: Optional[dict] = None        # request body for POST/PUT/PATCH
    path_params: dict[str, str] = {}   # e.g. {"id": "42"} for /users/{id}


class RestServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    host: str                          # e.g. "https://api.example.com"
    headers: dict[str, str] = {}
    endpoints: list[RestEndpoint]
    auto_start: bool = False
    idle_timeout: int = 300


# ──────────────────────────────────────────────
# Type 3 – Pre-configured: GitHub
# ──────────────────────────────────────────────

class GithubServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    personal_access_token: str
    auto_start: bool = False
    idle_timeout: int = 300


# ──────────────────────────────────────────────
# Type 3 – Pre-configured: Slack
# ──────────────────────────────────────────────

class SlackServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    bot_token: str                     # xoxb-...
    auto_start: bool = False
    idle_timeout: int = 300


# ──────────────────────────────────────────────
# Type 3 – Pre-configured: Gmail (OAuth2)
# ──────────────────────────────────────────────

class GmailServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    client_id: str
    client_secret: str
    refresh_token: str                 # pre-obtained via OAuth2 flow
    auto_start: bool = False
    idle_timeout: int = 300


# ──────────────────────────────────────────────
# Ollama Chat
# ──────────────────────────────────────────────

class OllamaMessage(BaseModel):
    role: str                          # "user" | "assistant" | "system"
    content: str


class OllamaChatRequest(BaseModel):
    model: str                         # any model installed in Ollama
    chat_history: list[OllamaMessage] = []
    chat_message: str
    tool: Optional[str] = None         # name of the MCP server to use as tool
    stream: bool = False
