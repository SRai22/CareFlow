import psycopg
from psycopg.types.json import Jsonb

from app.db import open_connection
from app.idempotency import request_hash
from app.schemas import EventCreate, EventRead


def migrate(connection: psycopg.Connection) -> None:
    # Historical successes used the same fixed 201 response contract.
    with connection.transaction():
        connection.execute("""
            ALTER TABLE events
                ADD COLUMN request_hash CHAR(64),
                ADD COLUMN response_status SMALLINT CHECK (response_status BETWEEN 100 AND 599),
                ADD COLUMN response_body JSONB
        """)
        events = connection.execute("""
            SELECT * FROM events
        """).fetchall()
        for stored in events:
            event = EventCreate(**{name: stored[name] for name in EventCreate.model_fields if name in stored})
            outcome = EventRead(**event.model_dump(), received_at=stored["received_at"]).model_dump(mode="json", exclude_none=True)
            connection.execute(
                """
                UPDATE events
                SET request_hash = %s, response_status = 201, response_body = %s
                WHERE event_id = %s
                """,
                (request_hash(event), Jsonb(outcome), event.event_id),
            )
        connection.execute("""
            ALTER TABLE events
                ALTER COLUMN request_hash SET NOT NULL,
                ALTER COLUMN response_status SET NOT NULL,
                ALTER COLUMN response_body SET NOT NULL
        """)


def main() -> None:
    with open_connection() as connection:
        migrate(connection)
    print("Added request fingerprints and stored outcomes to CareFlow events.")


if __name__ == "__main__":
    main()
