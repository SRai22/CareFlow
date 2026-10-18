from psycopg.types.json import Jsonb

from app.db import open_connection
from app.idempotency import request_hash
from app.schemas import EventCreate


class ProviderError(Exception):
    pass


def send_notification(idempotency_key: str, event: EventCreate) -> str:
    fingerprint = request_hash(event)
    # Separate autocommit connection models acceptance by an external provider.
    with open_connection() as connection:
        accepted = connection.execute(
            """
            INSERT INTO mock_provider_acceptances (idempotency_key, payload_hash, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """,
            (idempotency_key, fingerprint, Jsonb(event.model_dump(mode="json"))),
        ).fetchone()
        if accepted is None:
            accepted = connection.execute(
                "SELECT * FROM mock_provider_acceptances WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        if accepted["payload_hash"] != fingerprint:
            raise ValueError("Provider idempotency key reused with different content")
        return str(accepted["message_id"])
