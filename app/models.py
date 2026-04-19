from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime
from enum import Enum

class ServerType(str, Enum):
    command = "command"  
    rest_api = "rest_api"
    github = "github"    
    slack = "slack"      
    gmail = "gmail"      


class HttpMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"

class MCPServer(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    server_type: ServerType
    auto_start: bool = False         
    idle_timeout: int = 300           
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class CommandServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    command: str                      
    args: list[str] = []            
    env: dict[str, str] = {}         
    auto_start: bool = False
    idle_timeout: int = 300


class EndpointParam(BaseModel):
    name: str
    value: Any
    param_type: str = "query"          


class RestEndpoint(BaseModel):
    path: str                        
    method: HttpMethod
    description: Optional[str] = None
    params: list[EndpointParam] = []


class RestServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    host: str                        
    headers: dict[str, str] = {}
    endpoints: list[RestEndpoint]
    auto_start: bool = False
    idle_timeout: int = 300


class GithubServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    personal_access_token: str
    auto_start: bool = False
    idle_timeout: int = 300


class SlackServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    bot_token: str                    
    auto_start: bool = False
    idle_timeout: int = 300


class GmailServerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    client_id: str
    client_secret: str
    refresh_token: str                
    auto_start: bool = False
    idle_timeout: int = 300


class OllamaMessage(BaseModel):
    role: str                         
    content: str

class OllamaChatRequest(BaseModel):
    model: str                     
    chat_history: list[OllamaMessage] = []
    chat_message: str
    tool: Optional[str] = None         
    stream: bool = False