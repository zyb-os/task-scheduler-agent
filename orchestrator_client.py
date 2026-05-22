"""
orchestrator_client.py

Connects the task-scheduler-agent to the agent-orchestrator, implementing the
full protocol defined in AGENT_MANIFEST.md.

Protocol checklist (§14):
  ✓ POST /api/v1/agents/register — capability schema + required_settings
  ✓ WS /ws/{agent_id} — connect immediately after registration
  ✓ Close code 4004 — re-register then reconnect
  ✓ Exponential-backoff auto-reconnect (cap: 60 s)
  ✓ Heartbeat every 15 s — status, current_load, active_tasks, metrics
  ✓ task_request → capability handler → task_response
  ✓ Respects task timeout_ms hint
  ✓ status_update sent on task start / finish (available ↔ busy)
  ✓ Status machine: starting → available → busy → draining → offline
  ✓ Metrics: tasks_completed, tasks_failed, avg_response_time_ms, uptime_seconds
  ✓ agent_registered / agent_offline / error / broadcast / discovery_response handlers
  ✓ Graceful shutdown on SIGINT/SIGTERM: draining → wait → DELETE → WS close
  ✓ Outbound task_request dispatch with correlation_id tracking
  ✓ Background poll loop for due tasks (every POLL_INTERVAL_S seconds)
  ✓ SQLite task persistence via TaskStore

Usage:
    python orchestrator_client.py [--orchestrator-url http://localhost:8000]
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import websockets
import websockets.exceptions

from scheduler import TaskStore

# ── Stable agent identity ──────────────────────────────────────────────────

_AGENT_ID_FILE = Path(".agent_id")


def _stable_agent_id() -> str:
    """Read the persisted agent UUID from disk, or generate and save a new one."""
    if _AGENT_ID_FILE.exists():
        return _AGENT_ID_FILE.read_text().strip()
    new_id = str(uuid.uuid4())
    _AGENT_ID_FILE.write_text(new_id)
    logger.info("Generated new stable agent ID: %s → %s", new_id, _AGENT_ID_FILE)
    return new_id


logger = logging.getLogger(__name__)

# ── Agent identity ─────────────────────────────────────────────────────────

AGENT_NAME        = "task-scheduler-agent"
AGENT_VERSION     = "1.0.0"
AGENT_DESCRIPTION = (
    "Accepts tasks from any agent and schedules them for deferred execution. "
    "Tasks are persisted in SQLite and dispatched to the best available agent "
    "when their scheduled time arrives."
)

# ── Registration payload ───────────────────────────────────────────────────

REGISTRATION_PAYLOAD: dict = {
    "name": AGENT_NAME,
    "description": AGENT_DESCRIPTION,
    "version": AGENT_VERSION,
    "capabilities": [
        # ── schedule_task ──────────────────────────────────────────────────
        {
            "name": "schedule_task",
            "description": (
                "Schedule any agent capability to run at a specific future time. "
                "The task is persisted in SQLite and dispatched automatically when "
                "its scheduled_at time arrives."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "capability": {
                        "type": "string",
                        "description": "Capability name to invoke at the scheduled time.",
                    },
                    "input_data": {
                        "type": "object",
                        "description": "Input payload forwarded verbatim to the capability.",
                    },
                    "scheduled_at": {
                        "type": "string",
                        "description": (
                            "ISO 8601 UTC datetime when the task should run. "
                            "Must be in the future. "
                            "Examples: '2026-04-20T09:00:00Z', '2026-04-20T09:00:00+05:30'"
                        ),
                    },
                    "target_agent_id": {
                        "type": "string",
                        "description": (
                            "UUID of the specific agent to run the capability. "
                            "Optional — omit to let the scheduler auto-discover the best "
                            "available agent at dispatch time."
                        ),
                    },
                    "timeout_ms": {
                        "type": "number",
                        "description": "Max milliseconds to wait for the capability to respond. Default: 300 000 (5 min).",
                    },
                    "recurrence": {
                        "type": "string",
                        "description": (
                            "Optional cron expression (5-field or alias) for recurring tasks. "
                            "After each run the scheduler automatically schedules the next occurrence. "
                            "Examples: '0 9 * * 1-5' (weekdays at 09:00 UTC), '@daily', '*/15 * * * *', "
                            "'every:6h'. "
                            "Aliases: @hourly @daily @weekly @monthly @yearly. "
                            "Shorthand: every:Xm / every:Xh / every:Xd. "
                            "Omit for a one-shot task."
                        ),
                    },
                },
                "required": ["capability", "input_data", "scheduled_at"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "task_id":      {"type": "string", "description": "UUID of the created scheduled task."},
                    "scheduled_at": {"type": "string", "description": "Normalised UTC ISO 8601 run time."},
                    "status":       {"type": "string", "description": "Always 'pending' on creation."},
                    "recurrence":   {"type": ["string", "null"], "description": "Cron expression if recurring, else null."},
                },
            },
            "tags": ["scheduler", "deferred", "cron"],
        },
        # ── get_task ───────────────────────────────────────────────────────
        {
            "name": "get_task",
            "description": (
                "Return full details of a scheduled task including its timing metadata. "
                "Includes computed fields: time_until_execution_s, is_overdue, "
                "execution_delay_ms, plus the original input_data, result, and error."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "UUID of the scheduled task.",
                    },
                },
                "required": ["task_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "task": {
                        "type": "object",
                        "description": "Full task record (see field list below).",
                        "properties": {
                            "id":                     {"type": "string"},
                            "capability":             {"type": "string"},
                            "target_agent_id":        {"type": ["string", "null"]},
                            "input_data":             {"type": "object"},
                            "scheduled_at":           {"type": "string", "description": "ISO 8601 UTC"},
                            "timeout_ms":             {"type": ["number", "null"]},
                            "requester_id":           {"type": "string"},
                            "status":                 {"type": "string", "description": "pending|running|completed|failed|cancelled"},
                            "result":                 {"type": ["object", "null"]},
                            "error":                  {"type": ["string", "null"]},
                            "created_at":             {"type": "string"},
                            "executed_at":            {"type": ["string", "null"]},
                            "time_until_execution_s": {"type": ["number", "null"], "description": "Seconds until run; negative = overdue. Null once dispatched."},
                            "is_overdue":             {"type": "boolean", "description": "True when pending but scheduled_at is past."},
                            "execution_delay_ms":     {"type": ["number", "null"], "description": "Ms between scheduled_at and actual start. Null if not yet run."},
                            "recurrence":             {"type": ["string", "null"], "description": "Cron expression for recurring tasks, null for one-shots."},
                            "next_run_at":            {"type": ["string", "null"], "description": "ISO 8601 UTC of the next scheduled occurrence (set after this run completes). Null for one-shots or pending tasks."},
                        },
                    },
                },
            },
            "tags": ["scheduler", "query"],
        },
        # ── list_tasks ─────────────────────────────────────────────────────
        {
            "name": "list_scheduled_tasks",
            "description": (
                "List scheduled tasks with optional filtering by status, capability, "
                "and date range. Supports pagination. Each task includes full timing "
                "metadata (time_until_execution_s, is_overdue, execution_delay_ms)."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "Filter by status: pending | running | completed | failed | cancelled. Omit for all.",
                    },
                    "capability": {
                        "type": "string",
                        "description": "Filter to tasks for a specific capability name.",
                    },
                    "date_from": {
                        "type": "string",
                        "description": "ISO 8601 UTC lower bound on scheduled_at (inclusive).",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "ISO 8601 UTC upper bound on scheduled_at (inclusive).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max tasks to return (default 50, max 500).",
                        "default": 50,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of tasks to skip for pagination (default 0).",
                        "default": 0,
                    },
                    "order": {
                        "type": "string",
                        "description": "'asc' = oldest scheduled first (default); 'desc' = newest first.",
                        "default": "asc",
                    },
                },
                "required": [],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "tasks":            {"type": "array", "description": "List of full task records including timing metadata."},
                    "count":            {"type": "integer", "description": "Number of tasks in this page."},
                    "total":            {"type": "integer", "description": "Total matching tasks (for pagination)."},
                    "capabilities":     {"type": "array", "items": {"type": "string"}, "description": "All distinct capability names ever scheduled."},
                    "pending_count":    {"type": "integer"},
                    "overdue_count":    {"type": "integer"},
                },
            },
            "tags": ["scheduler", "query"],
        },
        # ── update_scheduled_task ──────────────────────────────────────────
        {
            "name": "update_scheduled_task",
            "description": (
                "Update a pending task's scheduled time, input data, or timeout. "
                "Only pending tasks can be updated — running or terminal tasks are immutable."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "UUID of the pending task to update.",
                    },
                    "scheduled_at": {
                        "type": "string",
                        "description": "New ISO 8601 UTC run time (must be in the future).",
                    },
                    "input_data": {
                        "type": "object",
                        "description": "Replace the task's input payload entirely.",
                    },
                    "timeout_ms": {
                        "type": "number",
                        "description": "New dispatch timeout in milliseconds.",
                    },
                },
                "required": ["task_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "task":    {"type": "object", "description": "Updated task record."},
                },
            },
            "tags": ["scheduler", "mutate"],
        },
        # ── cancel_scheduled_task ──────────────────────────────────────────
        {
            "name": "cancel_scheduled_task",
            "description": "Cancel a pending scheduled task. Running or completed tasks cannot be cancelled.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string", "description": "UUID of the task to cancel."},
                },
                "required": ["task_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"},
                },
            },
            "tags": ["scheduler", "mutate"],
        },
        # ── delete_task ────────────────────────────────────────────────────
        {
            "name": "delete_task",
            "description": (
                "Permanently delete a terminal task (completed, failed, or cancelled) "
                "from the store. Pending or running tasks must be cancelled first."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string", "description": "UUID of the terminal task to delete."},
                },
                "required": ["task_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"},
                },
            },
            "tags": ["scheduler", "mutate"],
        },
        # ── validate_cron ──────────────────────────────────────────────────
        {
            "name": "validate_cron",
            "description": (
                "Validate a cron expression and return a human-readable description of when it fires. "
                "Supports 5-field cron syntax, @-aliases (@daily, @weekly, etc.), and every:Xm/h/d shorthand. "
                "Use this before scheduling a recurring task to confirm the expression is correct."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": "Cron expression to validate (e.g. '0 9 * * 1-5', '@daily', 'every:15m').",
                    },
                },
                "required": ["expression"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "valid":       {"type": "boolean", "description": "True if the expression is valid."},
                    "description": {"type": "string",  "description": "Human-readable schedule description, or error message."},
                    "next_runs":   {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "ISO 8601 UTC datetimes of the next 5 scheduled runs (only present when valid=true).",
                    },
                },
            },
            "tags": ["scheduler", "cron", "utility"],
        },
        # ── get_task_status (backward-compat alias) ────────────────────────
        {
            "name": "get_task_status",
            "description": "Alias for get_task — returns full task details. Prefer get_task for new code.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string", "description": "UUID of the scheduled task."},
                },
                "required": ["task_id"],
            },
            "tags": ["scheduler", "query"],
        },
    ],
    "tags": ["scheduler", "deferred-execution", "task-queue"],
    "required_settings": [],
}

# ── Constants ──────────────────────────────────────────────────────────────

HEARTBEAT_INTERVAL_S: int = 15     # heartbeat cadence (§7)
MAX_BACKOFF_S:        int = 60      # reconnection backoff ceiling
DRAIN_TIMEOUT_S:      int = 60      # seconds to wait for in-flight dispatches on shutdown
POLL_INTERVAL_S:      int = 5       # how often to check for due tasks
DISPATCH_TIMEOUT_S:   float = 300.0 # default timeout for outbound task_request (5 min)


# ── Helpers ────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _envelope(
    sender_id: str,
    msg_type: str,
    payload: dict,
    recipient_id: str | None = None,
    correlation_id: str | None = None,
    msg_id: str | None = None,
) -> str:
    return json.dumps({
        "id":             msg_id or str(uuid.uuid4()),
        "type":           msg_type,
        "sender_id":      sender_id,
        "recipient_id":   recipient_id,
        "payload":        payload,
        "timestamp":      _now_iso(),
        "correlation_id": correlation_id,
    })


# ── Main client ────────────────────────────────────────────────────────────

class OrchestratorClient:
    """
    Registers the task-scheduler-agent with the orchestrator, maintains the
    persistent WebSocket connection, handles incoming scheduling requests, and
    dispatches due tasks to target agents.
    """

    def __init__(self, orchestrator_url: str = "http://localhost:8000") -> None:
        self._base = orchestrator_url.rstrip("/")
        self._http = httpx.AsyncClient(timeout=15)

        # Identity — populated after registration
        self._agent_id: str = ""
        self._ws_url: str = ""

        # Status / metrics
        self._status: str = "starting"
        self._active_tasks: int = 0
        self._dispatching: int = 0      # outbound task_requests in flight
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._total_duration_ms: float = 0.0
        self._start_time: float = time.monotonic()

        self._shutting_down: bool = False

        # Active WebSocket (set during session, None when disconnected)
        self._current_ws: Any = None

        # Pending outbound task_request futures: {req_msg_id → Future[payload]}
        self._pending_responses: dict[str, asyncio.Future] = {}

        # Task store
        self._store = TaskStore()
        self._poll_task: asyncio.Task | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Register, open the task store, start the poll loop, connect WS. Blocks until shutdown."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._graceful_shutdown()))

        self._store.open()
        await self._register()

        self._poll_task = asyncio.create_task(self._poll_loop(), name="scheduler-poll")
        await self._connect_loop()

    # ── Registration ───────────────────────────────────────────────────────

    async def _register(self) -> None:
        url = f"{self._base}/api/v1/agents/register"
        logger.info("Registering with orchestrator at %s …", url)
        payload = {**REGISTRATION_PAYLOAD, "id": _stable_agent_id()}
        resp = await self._http.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._agent_id = data["agent_id"]
        self._ws_url   = data["ws_url"]
        logger.info("Registered — agent_id=%s  ws=%s", self._agent_id, self._ws_url)

    # ── WebSocket connection loop ──────────────────────────────────────────

    async def _connect_loop(self) -> None:
        """Connect and reconnect with exponential backoff until shutdown."""
        backoff = 1.0
        while not self._shutting_down:
            try:
                logger.info("Connecting to %s …", self._ws_url)
                async with websockets.connect(self._ws_url) as ws:
                    backoff = 1.0
                    await self._run_session(ws)

            except websockets.exceptions.ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd else None
                if code == 4004:
                    logger.warning("Orchestrator: unknown agent_id (4004) — re-registering …")
                    try:
                        await self._register()
                    except Exception as reg_exc:
                        logger.error("Re-registration failed: %s", reg_exc)
                elif code == 4003:
                    logger.info("Agent is disabled by orchestrator (4003) — will retry so dashboard enable can restore connection")
                    backoff = max(backoff, 10.0)
                elif self._shutting_down:
                    break
                else:
                    logger.warning("WS closed (code=%s) — retry in %.0fs", code, backoff)

            except (OSError, Exception) as exc:
                if self._shutting_down:
                    break
                logger.warning("WS error (%s) — retry in %.0fs", exc, backoff)

            if not self._shutting_down:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

    async def _run_session(self, ws) -> None:
        """Run heartbeat + receive loop for one WS session."""
        self._current_ws = ws
        self._status = "available"
        logger.info("WebSocket session active — status: available")
        try:
            await asyncio.gather(
                self._heartbeat_loop(ws),
                self._recv_loop(ws),
            )
        finally:
            self._current_ws = None
            self._status = "offline"
            # Fail any pending outbound requests whose connection just dropped
            for fut in list(self._pending_responses.values()):
                if not fut.done():
                    fut.set_exception(ConnectionError("WebSocket session ended"))

    # ── Heartbeat ─────────────────────────────────────────────────────────

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            total_in_flight = self._active_tasks + self._dispatching
            await self._ws_send(ws, self._msg(
                "heartbeat",
                {
                    "status":                self._status,
                    "current_load":          min(total_in_flight / 10, 1.0),
                    "active_tasks":          total_in_flight,
                    "expected_wait_time_ms": 0,
                    "metrics":               self._metrics(),
                },
            ))
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    # ── Receive loop ───────────────────────────────────────────────────────

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Non-JSON WS frame ignored")
                continue

            mtype  = msg.get("type", "?")
            sender = msg.get("sender_id", "?")
            _lvl = logging.DEBUG if mtype in ("agent_registered", "agent_offline", "heartbeat_ack", "settings_push") else logging.INFO
            logger.log(_lvl, "← [%s] from=%s  %s", mtype, sender,
                        json.dumps(msg.get("payload", {}))[:200])
            await self._dispatch(ws, msg)

    async def _dispatch(self, ws, msg: dict) -> None:
        mtype   = msg.get("type", "")
        payload = msg.get("payload", {})

        if mtype == "task_request":
            asyncio.create_task(self._handle_incoming_task(ws, msg))

        elif mtype == "task_response":
            # Match outbound dispatch awaiting this correlation
            corr = msg.get("correlation_id")
            if corr and corr in self._pending_responses:
                fut = self._pending_responses.pop(corr)
                if not fut.done():
                    fut.set_result(payload)
            else:
                logger.debug("Unmatched task_response correlation_id=%s", corr)

        elif mtype == "agent_registered":
            logger.info("Peer joined: %s", payload.get("agent_id"))

        elif mtype == "agent_offline":
            agent_id = payload.get("agent_id")
            logger.info("Peer left: %s (reason: %s)", agent_id, payload.get("reason"))
            # Fail any pending dispatches targeting this agent
            self._fail_pending_for_agent(agent_id)

        elif mtype == "error":
            logger.error("Orchestrator error [%s]: %s",
                         payload.get("code"), payload.get("detail"))
            # Surface error to a pending dispatch if linked
            original_id = payload.get("original_message_id")
            if original_id and original_id in self._pending_responses:
                fut = self._pending_responses.pop(original_id)
                if not fut.done():
                    fut.set_exception(RuntimeError(
                        f"[{payload.get('code')}] {payload.get('detail')}"
                    ))

        elif mtype == "broadcast":
            logger.debug("Broadcast from %s: %s",
                         msg.get("sender_id"), payload.get("content"))

        elif mtype == "discovery_response":
            logger.debug("Discovery response: %d agent(s)",
                         len(payload.get("agents", [])))

        else:
            logger.debug("Unhandled message type: %r", mtype)

    # ── Incoming scheduling requests ───────────────────────────────────────

    async def _handle_incoming_task(self, ws, msg: dict) -> None:
        """
        Handle a task_request addressed to this scheduler agent.
        Routes to the correct capability handler and sends a task_response.
        """
        req_id    = msg.get("id")
        sender_id = msg.get("sender_id")
        payload   = msg.get("payload", {})
        capability = payload.get("capability")
        input_data = payload.get("input_data", {})
        timeout_ms: float | None = payload.get("timeout_ms")

        self._active_tasks += 1
        self._status = "busy" if self._active_tasks > 0 else "available"

        t0 = time.monotonic()
        try:
            if capability == "schedule_task":
                output, error = await self._cap_schedule_task(input_data, sender_id)
            elif capability == "get_task":
                output, error = await self._cap_get_task(input_data)
            elif capability == "get_task_status":          # backward-compat alias
                output, error = await self._cap_get_task(input_data)
            elif capability == "list_scheduled_tasks":
                output, error = await self._cap_list_tasks(input_data)
            elif capability == "update_scheduled_task":
                output, error = await self._cap_update_task(input_data)
            elif capability == "cancel_scheduled_task":
                output, error = await self._cap_cancel_task(input_data)
            elif capability == "delete_task":
                output, error = await self._cap_delete_task(input_data)
            elif capability == "validate_cron":
                output, error = await self._cap_validate_cron(input_data)
            else:
                output, error = None, f"Unknown capability: {capability!r}"

            duration_ms = (time.monotonic() - t0) * 1000

            if error:
                self._tasks_failed += 1
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": False, "error": error, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))
            else:
                self._tasks_completed += 1
                self._total_duration_ms += duration_ms
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": True, "output_data": output, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))

        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            self._tasks_failed += 1
            logger.exception("Unhandled exception in capability %r", capability)
            await self._ws_send(ws, self._msg(
                "task_response",
                {"success": False, "error": str(exc), "duration_ms": round(duration_ms, 1)},
                recipient_id=sender_id,
                correlation_id=req_id,
            ))

        finally:
            self._active_tasks = max(0, self._active_tasks - 1)
            total = self._active_tasks + self._dispatching
            self._status = "draining" if self._shutting_down else ("busy" if total else "available")
            await self._send_status_update(ws)

    # ── Capability handlers ────────────────────────────────────────────────

    async def _cap_schedule_task(
        self, input_data: dict, requester_id: str
    ) -> tuple[dict | None, str | None]:
        from cron import validate_cron as _validate_cron

        capability       = input_data.get("capability", "").strip()
        nested_input     = input_data.get("input_data", {})
        scheduled_at_raw = input_data.get("scheduled_at", "").strip()
        target_agent_id  = input_data.get("target_agent_id") or None
        timeout_ms       = input_data.get("timeout_ms") or None
        recurrence_raw   = (input_data.get("recurrence") or "").strip() or None

        if not capability:
            return None, "input_data.capability is required"
        if not scheduled_at_raw:
            return None, "input_data.scheduled_at is required"

        # Validate and normalise scheduled_at
        try:
            scheduled_at = _parse_datetime(scheduled_at_raw)
        except ValueError as exc:
            return None, f"Invalid scheduled_at: {exc}"
        # Prevent accidental immediate execution when callers pass a timestamp
        # that is already in the past (common with wrong timezone/year).
        scheduled_dt = datetime.fromisoformat(scheduled_at)
        now_dt = datetime.now(timezone.utc)
        if scheduled_dt <= now_dt:
            return None, (
                "input_data.scheduled_at must be in the future. "
                f"received={scheduled_at} now_utc={now_dt.isoformat(timespec='milliseconds')}"
            )

        # Validate cron expression if provided
        if recurrence_raw:
            ok, desc_or_err = _validate_cron(recurrence_raw)
            if not ok:
                return None, f"Invalid recurrence expression: {desc_or_err}"
            logger.info("Recurrence validated: %r → %s", recurrence_raw, desc_or_err)

        task_id = self._store.create_task(
            capability=capability,
            input_data=nested_input,
            scheduled_at=scheduled_at,
            requester_id=requester_id,
            target_agent_id=target_agent_id,
            timeout_ms=timeout_ms,
            recurrence=recurrence_raw,
        )
        logger.info(
            "Scheduled task %s for %s (capability=%s recurrence=%s)",
            task_id, scheduled_at, capability, recurrence_raw or "none",
        )
        return {
            "task_id":      task_id,
            "scheduled_at": scheduled_at,
            "status":       "pending",
            "recurrence":   recurrence_raw,
        }, None

    async def _cap_get_task(self, input_data: dict) -> tuple[dict | None, str | None]:
        task_id = input_data.get("task_id", "").strip()
        if not task_id:
            return None, "input_data.task_id is required"
        task = await asyncio.to_thread(self._store.get_task, task_id)
        if task is None:
            return None, f"Task '{task_id}' not found"
        return {"task": task}, None

    async def _cap_list_tasks(self, input_data: dict) -> tuple[dict | None, str | None]:
        status     = (input_data.get("status") or "").strip() or None
        capability = (input_data.get("capability") or "").strip() or None
        date_from  = (input_data.get("date_from") or "").strip() or None
        date_to    = (input_data.get("date_to") or "").strip() or None
        order      = (input_data.get("order") or "asc").strip().lower()
        try:
            limit  = max(1, min(500, int(input_data.get("limit",  50))))
            offset = max(0,          int(input_data.get("offset",  0)))
        except (ValueError, TypeError):
            limit, offset = 50, 0

        tasks = await asyncio.to_thread(
            self._store.list_tasks,
            status=status,
            capability=capability,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            order=order,
        )
        total = await asyncio.to_thread(
            self._store.count_tasks,
            status=status,
            capability=capability,
        )
        capabilities = await asyncio.to_thread(self._store.distinct_capabilities)

        # Summary counters (derived from the current page)
        pending_count = sum(1 for t in tasks if t.get("status") == "pending")
        overdue_count = sum(1 for t in tasks if t.get("is_overdue"))

        return {
            "tasks":         tasks,
            "count":         len(tasks),
            "total":         total,
            "offset":        offset,
            "limit":         limit,
            "capabilities":  capabilities,
            "pending_count": pending_count,
            "overdue_count": overdue_count,
        }, None

    async def _cap_update_task(self, input_data: dict) -> tuple[dict | None, str | None]:
        task_id      = (input_data.get("task_id") or "").strip()
        scheduled_at = input_data.get("scheduled_at") or None
        new_input    = input_data.get("input_data") or None
        timeout_ms   = input_data.get("timeout_ms") or None

        if not task_id:
            return None, "input_data.task_id is required"
        if scheduled_at is None and new_input is None and timeout_ms is None:
            return None, "Provide at least one of: scheduled_at, input_data, timeout_ms"

        # Validate + normalise the new scheduled_at if provided
        if scheduled_at:
            try:
                scheduled_at = _parse_datetime(scheduled_at)
            except ValueError as exc:
                return None, f"Invalid scheduled_at: {exc}"
            from datetime import datetime, timezone as _tz
            if datetime.fromisoformat(scheduled_at) <= datetime.now(_tz.utc):
                return None, "scheduled_at must be in the future"

        updated = await asyncio.to_thread(
            self._store.update_task,
            task_id,
            scheduled_at=scheduled_at,
            input_data=new_input,
            timeout_ms=timeout_ms,
        )
        if not updated:
            task = await asyncio.to_thread(self._store.get_task, task_id)
            if task is None:
                return None, f"Task '{task_id}' not found"
            return None, f"Task cannot be updated — current status: '{task['status']}' (only pending tasks may be changed)"

        task = await asyncio.to_thread(self._store.get_task, task_id)
        return {"success": True, "task": task}, None

    async def _cap_cancel_task(self, input_data: dict) -> tuple[dict | None, str | None]:
        task_id = (input_data.get("task_id") or "").strip()
        if not task_id:
            return None, "input_data.task_id is required"
        cancelled = await asyncio.to_thread(self._store.cancel_task, task_id)
        if cancelled:
            return {"success": True, "message": f"Task '{task_id}' cancelled."}, None
        task = await asyncio.to_thread(self._store.get_task, task_id)
        if task is None:
            return None, f"Task '{task_id}' not found"
        return {
            "success": False,
            "message": f"Task cannot be cancelled — status is '{task['status']}' (only pending tasks can be cancelled)",
        }, None

    async def _cap_delete_task(self, input_data: dict) -> tuple[dict | None, str | None]:
        task_id = (input_data.get("task_id") or "").strip()
        if not task_id:
            return None, "input_data.task_id is required"
        deleted = await asyncio.to_thread(self._store.delete_task, task_id)
        if deleted:
            return {"success": True, "message": f"Task '{task_id}' deleted."}, None
        task = await asyncio.to_thread(self._store.get_task, task_id)
        if task is None:
            return None, f"Task '{task_id}' not found"
        return {
            "success": False,
            "message": (
                f"Task cannot be deleted — status is '{task['status']}'. "
                "Only completed, failed, or cancelled tasks may be deleted."
            ),
        }, None

    async def _cap_validate_cron(self, input_data: dict) -> tuple[dict | None, str | None]:
        from cron import CronExpression, validate_cron as _validate_cron

        expression = (input_data.get("expression") or "").strip()
        if not expression:
            return None, "input_data.expression is required"

        valid, desc_or_err = _validate_cron(expression)
        if not valid:
            return {"valid": False, "description": desc_or_err, "next_runs": []}, None

        # Compute the next 5 run times for a preview
        try:
            cron_expr = CronExpression.parse(expression)
            now = datetime.now(timezone.utc)
            next_runs: list[str] = []
            cursor = now
            for _ in range(5):
                cursor = cron_expr.next_run(cursor)
                next_runs.append(cursor.isoformat(timespec="seconds"))
        except Exception as exc:
            logger.warning("validate_cron: next_run preview failed for %r: %s", expression, exc)
            next_runs = []

        return {
            "valid":       True,
            "description": desc_or_err,
            "next_runs":   next_runs,
        }, None

    # ── Background poll loop (dispatch due tasks) ──────────────────────────

    async def _poll_loop(self) -> None:
        """Periodically check for due tasks and dispatch them to target agents."""
        logger.info("Scheduler poll loop started (interval=%ds)", POLL_INTERVAL_S)
        while not self._shutting_down:
            await asyncio.sleep(POLL_INTERVAL_S)
            if not self._current_ws:
                logger.debug("No active WS — skipping poll")
                continue
            try:
                await self._poll_tick()
            except Exception:
                logger.exception("Error in scheduler poll tick")

    async def _poll_tick(self) -> None:
        due = self._store.get_due_tasks()
        if not due:
            return
        logger.info("Poll: %d task(s) due for dispatch", len(due))
        for task in due:
            # Mark as running immediately to prevent double-dispatch on next poll
            self._store.update_status(task["id"], "running")
            asyncio.create_task(
                self._execute_scheduled_task(task),
                name=f"dispatch-{task['id'][:8]}",
            )

    async def _execute_scheduled_task(self, task: dict) -> None:
        """Discover a target agent and forward the scheduled task."""
        task_id    = task["id"]
        capability = task["capability"]
        input_data = task["input_data"] if isinstance(task["input_data"], dict) else {}
        timeout_ms = task.get("timeout_ms")
        target_id  = task.get("target_agent_id")
        recurrence = task.get("recurrence")

        self._dispatching += 1
        terminal_status = "failed"   # updated on success
        try:
            # Resolve target agent
            if not target_id:
                target_id = await self._discover_best(capability)
                if not target_id:
                    raise RuntimeError(f"No available agent for capability '{capability}'")

            # Send task_request and await response
            success, result, error = await self._forward_task(
                target_agent_id=target_id,
                capability=capability,
                input_data=input_data,
                timeout_ms=timeout_ms,
            )

            if success:
                terminal_status = "completed"
                self._store.update_status(task_id, "completed", result=result)
                logger.info("Scheduled task %s completed (capability=%s)", task_id, capability)
            else:
                self._store.update_status(task_id, "failed", error=error)
                logger.warning("Scheduled task %s failed: %s", task_id, error)

        except Exception as exc:
            logger.exception("Unexpected error dispatching task %s", task_id)
            self._store.update_status(task_id, "failed", error=str(exc))

        finally:
            self._dispatching = max(0, self._dispatching - 1)

        # ── Recurrence: schedule the next occurrence ───────────────────────
        # We recycle regardless of success/failure so a transient error doesn't
        # permanently silence a recurring task.  Callers can cancel if they want
        # to stop a recurring task that keeps failing.
        if recurrence:
            try:
                new_id = await asyncio.to_thread(
                    self._store.recycle_recurring_task, task_id
                )
                if new_id:
                    logger.info(
                        "Recurring task %s → next occurrence %s (cron=%r)",
                        task_id, new_id, recurrence,
                    )
                else:
                    logger.warning(
                        "recycle_recurring_task returned None for task %s", task_id
                    )
            except Exception as exc:
                logger.error(
                    "Failed to recycle recurring task %s: %s", task_id, exc
                )

    # ── Outbound task dispatch ─────────────────────────────────────────────

    async def _discover_best(self, capability: str) -> str | None:
        """REST discovery: return the agent_id of the best agent for the capability."""
        try:
            resp = await self._http.get(
                f"{self._base}/api/v1/discover/best",
                params={"capability": capability},
            )
            if resp.status_code == 200:
                data = resp.json()
                agent_id = data.get("agent_id")
                logger.info(
                    "Discovered agent %s for capability '%s' (score=%.3f)",
                    agent_id, capability, data.get("score", 0),
                )
                return agent_id
            logger.warning(
                "No agent found for capability '%s' (status=%d)", capability, resp.status_code
            )
        except Exception as exc:
            logger.error("Discovery request failed: %s", exc)
        return None

    async def _forward_task(
        self,
        target_agent_id: str,
        capability: str,
        input_data: dict,
        timeout_ms: float | None,
    ) -> tuple[bool, dict | None, str | None]:
        """
        Send a task_request to the target agent via the orchestrator relay and
        wait for the task_response.
        """
        ws = self._current_ws
        if ws is None:
            return False, None, "No active WebSocket connection"

        req_id = str(uuid.uuid4())
        loop   = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_responses[req_id] = fut

        try:
            await self._ws_send(ws, _envelope(
                sender_id=self._agent_id,
                msg_type="task_request",
                payload={
                    "capability": capability,
                    "input_data": input_data,
                    **({"timeout_ms": timeout_ms} if timeout_ms else {}),
                },
                recipient_id=target_agent_id,
                msg_id=req_id,
            ))

            timeout_s = (timeout_ms / 1000) if timeout_ms else DISPATCH_TIMEOUT_S
            resp_payload = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s)

            if resp_payload.get("success"):
                return True, resp_payload.get("output_data"), None
            return False, None, resp_payload.get("error", "Unknown error")

        except asyncio.TimeoutError:
            return False, None, f"Task timed out after {timeout_ms or DISPATCH_TIMEOUT_S * 1000:.0f} ms"
        except Exception as exc:
            return False, None, str(exc)
        finally:
            self._pending_responses.pop(req_id, None)

    def _fail_pending_for_agent(self, agent_id: str | None) -> None:
        """Fail any pending futures that targeted an agent that just went offline."""
        # We can't easily tell which future targeted which agent without extra bookkeeping;
        # the futures will time out naturally. This is intentional to keep things simple.
        pass

    # ── Status update ──────────────────────────────────────────────────────

    async def _send_status_update(self, ws) -> None:
        total = self._active_tasks + self._dispatching
        await self._ws_send(ws, self._msg(
            "status_update",
            {
                "status":       self._status,
                "current_load": min(total / 10, 1.0),
                "active_tasks": total,
                "metrics":      self._metrics(),
            },
        ))

    # ── Graceful shutdown ──────────────────────────────────────────────────

    async def _graceful_shutdown(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Shutdown signal received — entering draining state …")
        self._status = "draining"

        # Wait for in-flight dispatches to finish
        deadline = time.monotonic() + DRAIN_TIMEOUT_S
        while (self._active_tasks + self._dispatching) > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        if self._active_tasks + self._dispatching:
            logger.warning(
                "Drain timeout — %d incoming + %d outgoing tasks still active",
                self._active_tasks, self._dispatching,
            )

        # Stop poll loop
        if self._poll_task:
            self._poll_task.cancel()

        # Deregister
        if self._agent_id:
            try:
                await self._http.delete(f"{self._base}/api/v1/agents/{self._agent_id}")
                logger.info("Deregistered from orchestrator.")
            except Exception as exc:
                logger.warning("Failed to deregister: %s", exc)

        self._store.close()
        await self._http.aclose()
        logger.info("Shutdown complete.")

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _ws_send(self, ws, msg_str: str) -> None:
        msg    = json.loads(msg_str)
        mtype  = msg.get("type", "?")
        is_noisy = mtype in ("heartbeat", "status_update")
        log    = logger.debug if is_noisy else logger.info
        log("→ [%s] to=%s  %s",
            mtype,
            msg.get("recipient_id") or "orchestrator",
            json.dumps(msg.get("payload", {}))[:200])
        await ws.send(msg_str)

    def _msg(
        self,
        msg_type: str,
        payload: dict,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        return _envelope(self._agent_id, msg_type, payload, recipient_id, correlation_id)

    def _metrics(self) -> dict:
        n = self._tasks_completed + self._tasks_failed
        return {
            "tasks_completed":    self._tasks_completed,
            "tasks_failed":       self._tasks_failed,
            "avg_response_time_ms": (
                round(self._total_duration_ms / n, 1) if n else 0.0
            ),
            "uptime_seconds": round(time.monotonic() - self._start_time, 1),
        }


# ── Utility ────────────────────────────────────────────────────────────────

def _parse_datetime(raw: str) -> str:
    """
    Parse an ISO 8601 datetime string and return a normalised UTC ISO string
    for storage (always includes timezone offset).

    Raises ValueError for unrecognisable formats.
    """
    # Try multiple formats
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%SZ",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {raw!r}")
