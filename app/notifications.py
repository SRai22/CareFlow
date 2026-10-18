from app.idempotency import request_hash
from app.mock_provider import send_notification
from app.schemas import EventCreate


def mark_sent(connection, event_id: str, notification_type: str, message_id: str) -> None:
    connection.execute(
        """
        UPDATE notifications
        SET status = 'SENT', provider_message_id = %s, sent_at = clock_timestamp()
        WHERE event_id = %s AND notification_type = %s
        """,
        (message_id, event_id, notification_type),
    )


def process_notification(event: EventCreate, connection) -> None:
    with connection.transaction():
        stored = connection.execute(
            "SELECT request_hash, notification_type FROM events WHERE event_id = %s",
            (event.event_id,),
        ).fetchone()
        if stored is None or stored["request_hash"] != request_hash(event):
            raise ValueError("Kafka event does not match a committed CareFlow event")
        notification_type = stored["notification_type"]
        if notification_type is None:
            return
        provider_key = f"{event.event_id}:{notification_type}"
        connection.execute(
            """
            INSERT INTO notifications (event_id, notification_type, provider_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id, notification_type) DO NOTHING
            """,
            (event.event_id, notification_type, provider_key),
        )

    # PENDING survives failure, while a row lock serializes duplicate deliveries.
    with connection.transaction():
        notification = connection.execute(
            """
            SELECT * FROM notifications
            WHERE event_id = %s AND notification_type = %s FOR UPDATE
            """,
            (event.event_id, notification_type),
        ).fetchone()
        if notification["status"] == "SENT":
            return
        message_id = send_notification(notification["provider_key"], event)
        mark_sent(connection, event.event_id, notification_type, message_id)
