from concurrent.futures import ThreadPoolExecutor
import argparse
import asyncio
import json
import logging

from temporalio.worker import Worker

from app.reminder_delivery import deliver_reminder, finish_uncertain_delivery
from app.temporal_client import connect_client, reminder_queue
from app.workflows.appointment_reminder import AppointmentReminderWorkflow


async def run(task_queue: str) -> None:
    client = await connect_client()
    with ThreadPoolExecutor(max_workers=4) as executor:
        async with Worker(
            client, task_queue=task_queue, workflows=[AppointmentReminderWorkflow],
            activities=[deliver_reminder, finish_uncertain_delivery], activity_executor=executor,
            max_concurrent_activities=4,
        ):
            print(json.dumps({"status": "ready", "task_queue": task_queue}), flush=True)
            await asyncio.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CareFlow Temporal workflow worker")
    parser.add_argument("--task-queue", default=reminder_queue())
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run(args.task_queue))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
