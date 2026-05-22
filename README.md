# Task Scheduler Agent

A deferred-task execution agent that connects to the **agent-orchestrator** and
allows any other agent to schedule tasks for later execution.

Tasks are persisted in a local **SQLite** database so they survive restarts.
When the scheduled time arrives the scheduler discovers the best available agent
for the requested capability and forwards the task via the orchestrator relay.

---

## Capabilities

| Capability | Description |
|---|---|
| `schedule_task` | Schedule a task for deferred execution |
| `cancel_scheduled_task` | Cancel a pending task by its ID |
| `get_task_status` | Query the current status and result of a task |
| `list_scheduled_tasks` | List tasks, optionally filtered by status |

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run (orchestrator must be reachable at localhost:8000)
python main.py

# Custom orchestrator URL
python main.py --orchestrator-url http://my-orchestrator:8000

# Verbose logging
python main.py --log-level DEBUG
```

---

## Scheduling a Task (from another agent)

Send a `task_request` to the scheduler agent's ID with capability `schedule_task`:

```json
{
  "type": "task_request",
  "sender_id": "<your-agent-id>",
  "recipient_id": "<scheduler-agent-id>",
  "payload": {
    "capability": "schedule_task",
    "input_data": {
      "capability":    "browse_web",
      "input_data":    {"task": "check price of AAPL stock"},
      "scheduled_at":  "2026-02-22T18:00:00Z",
      "timeout_ms":    30000
    }
  }
}
```

**Response:**

```json
{
  "success": true,
  "output_data": {
    "task_id":      "a1b2c3d4-…",
    "scheduled_at": "2026-02-22T18:00:00.000+00:00",
    "status":       "pending"
  }
}
```

### `schedule_task` input fields

| Field | Type | Required | Description |
|---|---|---|---|
| `capability` | string | ✓ | Capability name to invoke on the target agent |
| `input_data` | object | ✓ | Payload passed verbatim to the target capability |
| `scheduled_at` | string | ✓ | ISO 8601 UTC datetime — e.g. `2026-02-22T15:00:00Z` |
| `target_agent_id` | string | | Pin the task to a specific agent UUID. Omit to auto-discover. |
| `timeout_ms` | number | | Timeout hint (ms) forwarded to the target agent |

---

## Task Status Values

| Status | Meaning |
|---|---|
| `pending` | Waiting for its scheduled time |
| `running` | Currently being dispatched to a target agent |
| `completed` | Successfully executed |
| `failed` | Execution failed (see `error` field) |
| `cancelled` | Cancelled before execution |

---

## Checking Task Status

```json
{
  "capability": "get_task_status",
  "input_data": {"task_id": "a1b2c3d4-…"}
}
```

## Cancelling a Task

```json
{
  "capability": "cancel_scheduled_task",
  "input_data": {"task_id": "a1b2c3d4-…"}
}
```

## Listing Tasks

```json
{
  "capability": "list_scheduled_tasks",
  "input_data": {"status": "pending"}
}
```

Omit `status` to list all tasks.

---

## Architecture

```
Other Agent
    │
    │  task_request (schedule_task / cancel / status / list)
    ▼
Task Scheduler Agent
    │  ← persists to SQLite (tasks.db)
    │
    │  Background poll every 5 s
    │  ├── find due tasks (scheduled_at ≤ now, status = pending)
    │  ├── GET /api/v1/discover/best?capability=<name>  (or use stored target)
    │  └── task_request ──→ Orchestrator ──→ Target Agent
    │                                              │
    │  task_response ←──────────────────────────────
    │
    └── update task status: completed / failed
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ORCHESTRATOR_URL` | `http://localhost:8000` | Orchestrator base URL |
| `LOG_LEVEL` | `INFO` | Log verbosity |

---

## Files

| File | Purpose |
|---|---|
| `main.py` | Entry point and CLI |
| `orchestrator_client.py` | Orchestrator WebSocket client + capability handlers |
| `scheduler.py` | SQLite task store |
| `tasks.db` | Persisted task database (auto-created, git-ignored) |
| `.agent_id` | Stable agent UUID (auto-created, git-ignored) |
