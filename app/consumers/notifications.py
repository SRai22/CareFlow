from app.consumers import committed_event
from app.worker import run

GROUP_ID = "careflow-notifications"


def process_event(event, connection) -> None:
    with connection.transaction():
        notification_type = committed_event(event, connection)["notification_type"]
        if notification_type is None:
            return
        connection.execute(
            """
            INSERT INTO notifications (event_id, notification_type, provider_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id, notification_type) DO NOTHING
            """,
            (event.event_id, notification_type, f"{event.event_id}:{notification_type}"),
        )


def main() -> None:
    run(GROUP_ID, process_event)


if __name__ == "__main__":
    main()
