from app.idempotency import request_hash


def committed_event(event, connection):
    stored = connection.execute(
        "SELECT request_hash, notification_type FROM events WHERE event_id = %s",
        (event.event_id,),
    ).fetchone()
    if stored is None or stored["request_hash"] != request_hash(event):
        raise ValueError("Kafka event does not match a committed CareFlow event")
    return stored
