import argparse
import asyncio
import json
import logging

from temporalio.worker import Worker

from app.temporal_client import TASK_QUEUE, connect_client
from app.workflows.appointment_reminder import AppointmentReminderWorkflow


async def run(task_queue: str) -> None:
    client = await connect_client()
    async with Worker(client, task_queue=task_queue, workflows=[AppointmentReminderWorkflow]):
        print(json.dumps({"status": "ready", "task_queue": task_queue}), flush=True)
        await asyncio.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CareFlow Temporal workflow worker")
    parser.add_argument("--task-queue", default=TASK_QUEUE)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run(args.task_queue))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
