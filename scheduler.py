"""
scheduler.py

SQLite-backed task store for the task-scheduler-agent.

Stores scheduled tasks and exposes simple CRUD operations. All datetime
values are stored as ISO 8601 UTC strings for easy comparison.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from cron import next_cron_run, validate_cron

logger = logging.getLogger(__name__)

DB_PATH = Path("tasks.db")

# Task status values
STATUS_PENDING   = "pending"
STATUS_RUNNING   = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED    = "failed"
STATUS_CANCELLED = "cancelled"

TERMINAL_STATUSES = {STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class TaskStore:
    """Thread-safe SQLite store for scheduled tasks.

    Open with ``open()`` before use and close with ``close()`` on shutdown.
    All methods are synchronous; call them from a thread or via
    ``asyncio.to_thread`` if needed.
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def open(self) -> None:
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                id               TEXT PRIMARY KEY,
                capability       TEXT NOT NULL,
                target_agent_id  TEXT,
                input_data       TEXT NOT NULL,
                scheduled_at     TEXT NOT NULL,
                timeout_ms       REAL,
                requester_id     TEXT NOT NULL,
                status           TEXT NOT NULL DEFAULT 'pending',
                result           TEXT,
                error            TEXT,
                created_at       TEXT NOT NULL,
                executed_at      TEXT,
                recurrence       TEXT,
                next_run_at      TEXT
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_scheduled "
            "ON scheduled_tasks (status, scheduled_at)"
        )
        # Migration: add recurrence columns to pre-existing databases
        existing_cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(scheduled_tasks)")
        }
        for col, ddl in (
            ("recurrence",  "ALTER TABLE scheduled_tasks ADD COLUMN recurrence TEXT"),
            ("next_run_at", "ALTER TABLE scheduled_tasks ADD COLUMN next_run_at TEXT"),
        ):
            if col not in existing_cols:
                self._conn.execute(ddl)
                logger.info("Migrated DB: added column '%s' to scheduled_tasks", col)
        self._conn.commit()
        logger.info("Task store opened: %s", self._db_path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Write operations ───────────────────────────────────────────────────

    def create_task(
        self,
        capability: str,
        input_data: dict,
        scheduled_at: str,
        requester_id: str,
        target_agent_id: str | None = None,
        timeout_ms: float | None = None,
        recurrence: str | None = None,
    ) -> str:
        """Insert a new pending task and return its UUID."""
        task_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO scheduled_tasks
                (id, capability, target_agent_id, input_data, scheduled_at,
                 timeout_ms, requester_id, status, created_at, recurrence)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                task_id,
                capability,
                target_agent_id,
                json.dumps(input_data),
                scheduled_at,
                timeout_ms,
                requester_id,
                _now_iso(),
                recurrence or None,
            ),
        )
        self._conn.commit()
        logger.info(
            "Task created: id=%s  capability=%s  scheduled_at=%s  recurrence=%s",
            task_id, capability, scheduled_at, recurrence or "none",
        )
        return task_id

    def update_status(
        self,
        task_id: str,
        status: str,
        result: dict | None = None,
        error: str | None = None,
    ) -> None:
        executed_at = _now_iso() if status in (STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED) else None
        self._conn.execute(
            """
            UPDATE scheduled_tasks
            SET status = ?, result = ?, error = ?, executed_at = COALESCE(?, executed_at)
            WHERE id = ?
            """,
            (
                status,
                json.dumps(result) if result is not None else None,
                error,
                executed_at,
                task_id,
            ),
        )
        self._conn.commit()

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task. Returns True if the task was found and cancelled."""
        cur = self._conn.execute(
            "UPDATE scheduled_tasks SET status = 'cancelled' "
            "WHERE id = ? AND status = 'pending'",
            (task_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ── Read operations ────────────────────────────────────────────────────

    def get_due_tasks(self) -> list[dict]:
        """Return pending tasks whose scheduled_at is in the past, oldest first."""
        now = _now_iso()
        rows = self._conn.execute(
            """
            SELECT * FROM scheduled_tasks
            WHERE status = 'pending' AND scheduled_at <= ?
            ORDER BY scheduled_at ASC
            """,
            (now,),
        ).fetchall()
        return [self._deserialise(dict(row)) for row in rows]

    def get_task(self, task_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM scheduled_tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if not row:
            return None
        return self._enrich(self._deserialise(dict(row)))

    def list_tasks(
        self,
        status: str | None = None,
        capability: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 100,
        offset: int = 0,
        order: str = "asc",          # "asc" | "desc"
    ) -> list[dict]:
        """
        Return tasks with optional filters.  Results are sorted by
        ``scheduled_at`` ascending (oldest first) by default; pass
        ``order='desc'`` for newest-scheduled first.
        """
        clauses: list[str] = []
        params:  list      = []

        if status:
            clauses.append("status = ?")
            params.append(status)
        if capability:
            clauses.append("capability = ?")
            params.append(capability)
        if date_from:
            clauses.append("scheduled_at >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("scheduled_at <= ?")
            params.append(date_to)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        direction = "DESC" if order.lower() == "desc" else "ASC"
        sql = (
            f"SELECT * FROM scheduled_tasks {where} "
            f"ORDER BY scheduled_at {direction} "
            f"LIMIT ? OFFSET ?"
        )
        params.extend([max(1, limit), max(0, offset)])
        rows = self._conn.execute(sql, params).fetchall()
        return [self._enrich(self._deserialise(dict(row))) for row in rows]

    def count_tasks(
        self,
        status: str | None = None,
        capability: str | None = None,
    ) -> int:
        """Return total count matching optional filters (for pagination)."""
        clauses: list[str] = []
        params:  list      = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if capability:
            clauses.append("capability = ?")
            params.append(capability)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        row = self._conn.execute(
            f"SELECT COUNT(*) AS n FROM scheduled_tasks {where}", params
        ).fetchone()
        return int(row["n"]) if row else 0

    def update_task(
        self,
        task_id: str,
        scheduled_at: str | None = None,
        input_data: dict | None = None,
        timeout_ms: float | None = None,
    ) -> bool:
        """
        Update a *pending* task's scheduled time, input_data, or timeout.
        Returns True if the update was applied, False if the task is not
        pending (or does not exist).
        """
        sets:   list[str] = []
        params: list      = []

        if scheduled_at is not None:
            sets.append("scheduled_at = ?")
            params.append(scheduled_at)
        if input_data is not None:
            sets.append("input_data = ?")
            params.append(json.dumps(input_data))
        if timeout_ms is not None:
            sets.append("timeout_ms = ?")
            params.append(timeout_ms)

        if not sets:
            return False   # nothing to update

        params.append(task_id)
        cur = self._conn.execute(
            f"UPDATE scheduled_tasks SET {', '.join(sets)} "
            f"WHERE id = ? AND status = 'pending'",
            params,
        )
        self._conn.commit()
        return cur.rowcount > 0

    def delete_task(self, task_id: str) -> bool:
        """
        Hard-delete a task.  Only terminal tasks (completed/failed/cancelled)
        may be deleted.  Returns True if deleted, False otherwise.
        """
        cur = self._conn.execute(
            f"DELETE FROM scheduled_tasks "
            f"WHERE id = ? AND status IN ('completed','failed','cancelled')",
            (task_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def recycle_recurring_task(self, task_id: str) -> str | None:
        """
        For a completed or failed recurring task, compute its next scheduled run
        and insert a fresh pending copy.  Returns the new task's UUID, or None
        if the task has no recurrence expression or cannot be found.

        The *next* run is computed from the current wall-clock time, so late
        completions never cause a cascade of back-filled runs.
        """
        task = self.get_task(task_id)
        if task is None:
            logger.warning("recycle_recurring_task: task %s not found", task_id)
            return None

        recurrence = task.get("recurrence")
        if not recurrence:
            return None   # not a recurring task

        now = datetime.now(timezone.utc)
        try:
            next_dt = next_cron_run(recurrence, now)
        except Exception as exc:
            logger.error(
                "recycle_recurring_task: failed to compute next run for task %s "
                "expression=%r: %s", task_id, recurrence, exc,
            )
            return None

        next_iso = next_dt.isoformat(timespec="milliseconds")

        # Record next_run_at on the completed task for audit purposes
        self._conn.execute(
            "UPDATE scheduled_tasks SET next_run_at = ? WHERE id = ?",
            (next_iso, task_id),
        )
        self._conn.commit()

        new_id = self.create_task(
            capability=task["capability"],
            input_data=task["input_data"] if isinstance(task["input_data"], dict) else {},
            scheduled_at=next_iso,
            requester_id=task["requester_id"],
            target_agent_id=task.get("target_agent_id"),
            timeout_ms=task.get("timeout_ms"),
            recurrence=recurrence,
        )
        logger.info(
            "Recurring task recycled: old=%s new=%s next_run=%s cron=%r",
            task_id, new_id, next_iso, recurrence,
        )
        return new_id

    def distinct_capabilities(self) -> list[str]:
        """Return sorted list of all capability names that have ever been scheduled."""
        rows = self._conn.execute(
            "SELECT DISTINCT capability FROM scheduled_tasks ORDER BY capability ASC"
        ).fetchall()
        return [r["capability"] for r in rows]

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _deserialise(row: dict) -> dict:
        """Parse JSON fields back to Python objects."""
        for field in ("input_data", "result"):
            if row.get(field):
                try:
                    row[field] = json.loads(row[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return row

    @staticmethod
    def _enrich(task: dict) -> dict:
        """
        Add computed timing fields to a task dict so callers get a complete
        picture without doing date arithmetic themselves.

        Added fields
        ────────────
        time_until_execution_s  float | None
            Seconds until scheduled_at from now.  Negative means overdue.
            None for tasks that have already been dispatched (non-pending).

        is_overdue              bool
            True when status is 'pending' and scheduled_at is in the past.

        execution_delay_ms      float | None
            How many ms after scheduled_at the task actually started running.
            Only set for running/completed/failed tasks; None otherwise.

        Note: recurrence (cron expression) and next_run_at (ISO UTC of the
        freshly-created successor task) are DB columns passed through as-is.
        """
        now = datetime.now(timezone.utc)

        scheduled_str = task.get("scheduled_at")
        executed_str  = task.get("executed_at")
        status        = task.get("status", "")

        # ── time_until_execution_s ─────────────────────────────────────────
        if status == STATUS_PENDING and scheduled_str:
            try:
                scheduled_dt = datetime.fromisoformat(scheduled_str)
                if scheduled_dt.tzinfo is None:
                    scheduled_dt = scheduled_dt.replace(tzinfo=timezone.utc)
                task["time_until_execution_s"] = round(
                    (scheduled_dt - now).total_seconds(), 1
                )
            except (ValueError, TypeError):
                task["time_until_execution_s"] = None
        else:
            task["time_until_execution_s"] = None

        # ── is_overdue ─────────────────────────────────────────────────────
        if status == STATUS_PENDING and task.get("time_until_execution_s") is not None:
            task["is_overdue"] = task["time_until_execution_s"] < 0
        else:
            task["is_overdue"] = False

        # ── execution_delay_ms ─────────────────────────────────────────────
        if status in (STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED) \
                and scheduled_str and executed_str:
            try:
                sched_dt = datetime.fromisoformat(scheduled_str)
                exec_dt  = datetime.fromisoformat(executed_str)
                if sched_dt.tzinfo is None:
                    sched_dt = sched_dt.replace(tzinfo=timezone.utc)
                if exec_dt.tzinfo is None:
                    exec_dt = exec_dt.replace(tzinfo=timezone.utc)
                task["execution_delay_ms"] = round(
                    (exec_dt - sched_dt).total_seconds() * 1000, 1
                )
            except (ValueError, TypeError):
                task["execution_delay_ms"] = None
        else:
            task["execution_delay_ms"] = None

        return task
