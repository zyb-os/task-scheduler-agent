#!/usr/bin/env python3
"""
Task Scheduler Agent — entry point.

Registers with the agent-orchestrator and waits for scheduling requests
from other agents.  Tasks are persisted in SQLite and dispatched to the
best available agent when their scheduled time arrives.

Usage:
    python main.py [--orchestrator-url http://localhost:8000]
"""

import argparse
import asyncio
import logging
import os

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ── Banner ─────────────────────────────────────────────────────────────────

BANNER = """
╔═══════════════════════════════════════════╗
║       Task Scheduler Agent  v1.0.0        ║
║  Deferred task execution for any agent    ║
╚═══════════════════════════════════════════╝

Capabilities exposed:
  • schedule_task          — schedule a task for later execution
  • cancel_scheduled_task  — cancel a pending task by ID
  • get_task_status        — query status / result of a task
  • list_scheduled_tasks   — list tasks (with optional status filter)
"""


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Task Scheduler Agent — connects to the agent-orchestrator "
                    "and provides deferred task scheduling."
    )
    parser.add_argument(
        "--orchestrator-url",
        default=os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000"),
        help="Orchestrator base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    print(BANNER)
    print(f"Orchestrator: {args.orchestrator_url}\n")

    from orchestrator_client import OrchestratorClient

    async def _run() -> None:
        client = OrchestratorClient(orchestrator_url=args.orchestrator_url)
        await client.start()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
