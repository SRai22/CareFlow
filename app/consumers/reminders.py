import asyncio
import logging

from psycopg.types.json import Jsonb
from temporalio.service import RPCError

from app.consumers import committed_event
from app.temporal_client import TemporalHandoffError, cancel_reminder, connect_client, ensure_reminder
from app.worker import run

GROUP_ID = "careflow-reminders"
HANDOFF_TIMEOUT_SECONDS = 10
logger = logging.getLogger(__name__)


async def handoff(event, source, previous=None) -> dict:
    try:
        async with asyncio.timeout(HANDOFF_TIMEOUT_SECONDS):
            client = await connect_client()
            result = {}
            if previous is not None:
                result["previous"] = await cancel_reminder(client, previous) if previous["scheduled_for"] else "NO_REMINDER_TIME"
            if event.event_type == "APPOINTMENT_CANCELLED":
                result["cancellation"] = await cancel_reminder(client, source)
            else:
                handle = await ensure_reminder(
                    client, source["appointment_id"], source["scheduled_for"],
                    schedule_generation=source["schedule_generation"],
                    event_id=source["event_id"] if source["schedule_generation"] is not None else None,
                )
                result["workflow_id"] = handle.id
            return result
    except (RPCError, RuntimeError, TimeoutError) as exc:
        raise TemporalHandoffError("Temporal did not acknowledge the reminder handoff") from exc


def source_event(connection, appointment_id, generation):
    source = connection.execute(
        """SELECT event_id, appointment_id, schedule_generation, scheduled_for FROM events
           WHERE appointment_id = %s AND COALESCE(schedule_generation, 1) = %s
             AND event_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_RESCHEDULED')
           ORDER BY received_at, event_id LIMIT 1""", (appointment_id, generation),
    ).fetchone()
    if source is None:
        raise ValueError("Scheduling event for reminder generation is missing")
    return source


def process_event(event, connection) -> None:
    stored = committed_event(event, connection)
    if connection.execute("SELECT 1 FROM reminder_handoffs WHERE event_id = %s", (event.event_id,)).fetchone():
        return
    generation = stored["schedule_generation"] or 1
    source = source_event(connection, event.appointment_id, generation)
    if source["scheduled_for"] is None:
        logger.warning("Historical event %s has no appointment time; no reminder can be started", event.event_id)
        result = {"outcome": "NO_REMINDER_TIME"}
    else:
        previous = source_event(connection, event.appointment_id, generation - 1) if event.event_type == "APPOINTMENT_RESCHEDULED" else None
        result = asyncio.run(handoff(event, source, previous))
    # This receipt commits only after start/signal acknowledgment (or confirmed terminal execution).
    with connection.transaction():
        connection.execute(
            "INSERT INTO reminder_handoffs(event_id, outcome) VALUES (%s, %s) ON CONFLICT(event_id) DO NOTHING",
            (event.event_id, Jsonb(result)),
        )


def main() -> None:
    run(GROUP_ID, process_event)


if __name__ == "__main__":
    main()
