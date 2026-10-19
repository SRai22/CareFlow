import argparse
import asyncio
import json
import math

from pydantic import TypeAdapter, ValidationError
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.schemas import Identifier
from app.temporal_client import TASK_QUEUE, connect_client, start_reminder


async def run(args) -> None:
    client = await connect_client()
    handle = await start_reminder(
        client, args.appointment_id, args.delay_seconds, task_queue=args.task_queue,
    )
    print(json.dumps({"workflow_id": handle.id, "run_id": handle.result_run_id}), flush=True)
    if args.wait:
        print(await handle.result(), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the Milestone 4.1 durable timer demo")
    parser.add_argument("--appointment-id", required=True)
    parser.add_argument("--delay-seconds", type=float, default=30)
    parser.add_argument("--task-queue", default=TASK_QUEUE)
    parser.add_argument("--wait", action="store_true", help="Wait for the durable workflow result")
    args = parser.parse_args()
    try:
        TypeAdapter(Identifier).validate_python(args.appointment_id)
    except ValidationError:
        parser.error("--appointment-id must contain 1–128 letters, digits, underscores or hyphens")
    if not math.isfinite(args.delay_seconds) or args.delay_seconds <= 0:
        parser.error("--delay-seconds must be finite and positive")
    try:
        asyncio.run(run(args))
    except WorkflowAlreadyStartedError:
        parser.exit(1, "A workflow already exists for this appointment; no new execution was started.\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
