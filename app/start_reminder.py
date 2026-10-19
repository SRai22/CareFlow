import argparse
import asyncio
import json
from datetime import datetime, timezone

from pydantic import AwareDatetime, TypeAdapter, ValidationError
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.schemas import Identifier
from app.temporal_client import connect_client, reminder_queue, start_reminder


async def run(args) -> None:
    client = await connect_client()
    handle = await start_reminder(
        client, args.appointment_id, args.scheduled_for, task_queue=args.task_queue,
    )
    print(json.dumps({"workflow_id": handle.id, "run_id": handle.result_run_id}), flush=True)
    if args.wait:
        print(await handle.result(), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a reminder due 24 hours before an appointment")
    parser.add_argument("--appointment-id", required=True)
    parser.add_argument("--scheduled-for", required=True, help="Future ISO 8601 timestamp with timezone")
    parser.add_argument("--task-queue", default=reminder_queue())
    parser.add_argument("--wait", action="store_true", help="Wait for the durable workflow result")
    args = parser.parse_args()
    try:
        TypeAdapter(Identifier).validate_python(args.appointment_id)
        args.scheduled_for = TypeAdapter(AwareDatetime).validate_python(args.scheduled_for)
    except ValidationError:
        parser.error("Supply a valid appointment ID and timezone-aware --scheduled-for")
    if args.scheduled_for <= datetime.now(timezone.utc):
        parser.error("--scheduled-for must be in the future")
    try:
        asyncio.run(run(args))
    except WorkflowAlreadyStartedError:
        parser.exit(1, "A workflow already exists for this appointment; no new execution was started.\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
