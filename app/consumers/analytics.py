from app.consumers import committed_event
from app.worker import run

GROUP_ID = "careflow-analytics"


def process_event(event, connection) -> None:
    with connection.transaction():
        committed_event(event, connection)
        connection.execute(
            """
            INSERT INTO analytics_events (event_id, appointment_id, event_type, occurred_at)
            VALUES (%(event_id)s, %(appointment_id)s, %(event_type)s, %(occurred_at)s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event.model_dump(),
        )


def main() -> None:
    run(GROUP_ID, process_event)


if __name__ == "__main__":
    main()
