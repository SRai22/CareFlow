import asyncio
import logging

from temporalio.service import RPCError

from app.consumers import committed_event
from app.temporal_client import TemporalHandoffError, connect_client, ensure_reminder
from app.worker import run

GROUP_ID = "careflow-reminders"
HANDOFF_TIMEOUT_SECONDS = 10
logger = logging.getLogger(__name__)


async def handoff(event) -> None:
    try:
        async with asyncio.timeout(HANDOFF_TIMEOUT_SECONDS):
            client = await connect_client()
            await ensure_reminder(client, event.appointment_id, event.scheduled_for)
    except (RPCError, RuntimeError, TimeoutError) as exc:
        raise TemporalHandoffError("Temporal did not acknowledge the reminder handoff") from exc


def process_event(event, connection) -> None:
    committed_event(event, connection)
    if event.event_type == "APPOINTMENT_SCHEDULED":
        if event.scheduled_for is None:
            logger.warning("Historical event %s has no appointment time; no reminder can be started", event.event_id)
            return
        asyncio.run(handoff(event))


def main() -> None:
    run(GROUP_ID, process_event)


if __name__ == "__main__":
    main()
