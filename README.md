# MCP Gateway

![image](image.png)

A FastAPI backend that turns **three kinds of things** into SSE-based MCP servers accessible at a single unified endpoint:

```
POST /mcp_servers/{name}/mcp
```

Plus a built-in **Ollama** integration so a local LLM can call any of your MCP servers as a tool, with simple single page frontend.

---

## Project structure

```
mcp_gateway/
├── app/
│   ├── __init__.py
│   ├── main.py       ← FastAPI app + lifespan (DB init, auto_start)
│   ├── models.py     ← Pydantic models for all 3 types + Ollama
│   ├── db.py         ← aiosqlite CRUD helpers
│   ├── utils.py      ← All helper logic (process mgmt, SSE, API calls)
│   └── views.py      ← All HTTP routes
├── requirements.txt
└── README.md
```

---

## Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
# Swagger UI → http://localhost:8000/docs
```

---

## Type 1 – Command-based MCP server

### Create
```http
POST /mcp-servers/command
{
  "name":        "my-postgres",
  "description": "PostgreSQL CLI bridge",
  "command":     "psql",
  "args":        ["-U", "postgres", "-d", "mydb", "--host", "127.0.0.1"],
  "env":         { "PGPASSWORD": "secret" },
  "auto_start":  false,
  "idle_timeout": 300
}
```

### Call (SSE)
```http
POST /mcp_servers/my-postgres/mcp
{ "payload": { "query": "SELECT now();" } }
```

The server stays alive for `idle_timeout` seconds after the last call, then auto-kills.

---

## Type 2 – REST API MCP server

### Create
```http
POST /mcp-servers/rest-api
{
  "name":  "jsonplaceholder",
  "host":  "https://jsonplaceholder.typicode.com",
  "headers": {},
  "endpoints": [
    {
      "path":   "/todos/{id}",
      "method": "GET",
      "description": "Get a todo by ID",
      "params": [
        { "name": "id", "value": 1, "param_type": "path" }
      ]
    }
  ]
}
```

### Call (SSE)
```http
POST /mcp_servers/jsonplaceholder/mcp
{
  "endpoint": "/todos/1",
  "method":   "GET",
  "params":   {}
}
```

---

## Type 3 – GitHub

### Create
```http
POST /mcp-servers/github
{
  "name":                  "my-github",
  "personal_access_token": "ghp_xxxxxxxxxxxx"
}
```

### Call (SSE)
```http
POST /mcp_servers/my-github/mcp
{
  "action": "list_repos",
  "params": { "per_page": 10 }
}
```

Available actions: `list_repos`, `get_repo`, `list_issues`, `get_issue`, `list_prs`, `get_pr`, `list_branches`, `list_commits`

Required params per action:
| Action | Required params |
|---|---|
| `get_repo` | `owner`, `repo` |
| `list_issues` | `owner`, `repo` |
| `get_issue` | `owner`, `repo`, `number` |
| `list_prs` | `owner`, `repo` |
| `get_pr` | `owner`, `repo`, `number` |
| `list_branches` | `owner`, `repo` |
| `list_commits` | `owner`, `repo` |

---

## Type 3 – Slack

### Create
```http
POST /mcp-servers/slack
{
  "name":      "my-slack",
  "bot_token": "xoxb-xxxxxxxxxxxx"
}
```

### Call (SSE)
```http
POST /mcp_servers/my-slack/mcp
{
  "action": "post_message",
  "params": { "channel": "C0123456789", "text": "Hello from MCP Gateway!" }
}
```

Available actions: `list_channels`, `post_message`, `get_messages`, `get_user`, `list_users`

---

## Type 3 – Gmail (OAuth2)

You need to obtain a `refresh_token` once via Google's OAuth2 flow (e.g. using `google-auth-oauthlib` or the Playground at https://developers.google.com/oauthlib/playground).

### Create
```http
POST /mcp-servers/gmail
{
  "name":          "my-gmail",
  "client_id":     "xxxxxxx.apps.googleusercontent.com",
  "client_secret": "xxxxxxx",
  "refresh_token": "1//xxxxxxx"
}
```

### Call (SSE)
```http
POST /mcp_servers/my-gmail/mcp
{
  "action": "list_messages",
  "params": { "q": "is:unread", "maxResults": 5 }
}
```

Available actions: `list_messages`, `get_message`, `send_message`, `list_labels`, `get_profile`

For `send_message` params: `{ "to": "...", "subject": "...", "body": "..." }`

---

## Ollama

### List installed models
```http
GET /ollama/models
```

### Chat (no streaming)
```http
POST /ollama/chat
{
  "model":        "llama3",
  "chat_history": [
    { "role": "user",      "content": "Hi!" },
    { "role": "assistant", "content": "Hello! How can I help?" }
  ],
  "chat_message": "List my GitHub repos",
  "tool":         "my-github",
  "stream":       false
}
```

### Chat (streaming SSE)
```http
POST /ollama/chat
{
  "model":        "llama3",
  "chat_message": "What's the weather like?",
  "stream":       true
}
```

When `tool` is provided, the named MCP server's metadata is injected as a system prompt so the model is aware of its capabilities. You then call the tool endpoint yourself based on the model's response.

---

## Process management (Type 1 only)

```http
# Kill a running process immediately
DELETE /mcp_servers/{name}/kill

# See which processes are currently alive
GET /mcp_servers/running
```

---

## SSE response format

Every `/mcp_servers/{name}/mcp` endpoint returns `text/event-stream`. Events:

```
event: message
data: {"status_code": 200, "data": {...}}

event: done
data: {"status": "done"}

event: error
data: {"error": "something went wrong"}
```