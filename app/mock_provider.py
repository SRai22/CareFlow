import hashlib
import json
import os

from psycopg.types.json import Jsonb

from app.db import open_connection
from app.schemas import EventCreate


class ProviderError(Exception):
    pass


def send_notification(idempotency_key: str, event: EventCreate) -> str:
    return send_payload(idempotency_key, event.model_dump(mode="json", exclude_none=True))


def send_payload(idempotency_key: str, payload: dict) -> str:
    if os.environ.get("MOCK_PROVIDER_FAIL_BEFORE_ACCEPT") == "1":
        raise ProviderError("Injected provider unavailability")
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    # Separate autocommit connection models acceptance by an external provider.
    with open_connection() as connection:
        accepted = connection.execute(
            """
            INSERT INTO mock_provider_acceptances (idempotency_key, payload_hash, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """,
            (idempotency_key, fingerprint, Jsonb(payload)),
        ).fetchone()
        newly_accepted = accepted is not None
        if accepted is None:
            accepted = connection.execute(
                "SELECT * FROM mock_provider_acceptances WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        if accepted["payload_hash"] != fingerprint:
            raise ValueError("Provider idempotency key reused with different content")
        message_id = str(accepted["message_id"])
    if newly_accepted and os.environ.get("MOCK_PROVIDER_TIMEOUT_AFTER_ACCEPT_ONCE") == "1":
        raise ProviderError("Injected timeout after durable provider acceptance")
    return message_id
