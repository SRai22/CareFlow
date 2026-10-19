import logging
from time import sleep

import psycopg

from app.db import open_connection
from app.kafka_producer import KafkaPublisher, PublishError

logger = logging.getLogger(__name__)


def publish_one(connection, publisher) -> bool:
    with connection.transaction():
        # shortcut: multiple relays can reorder one appointment; run a single relay for now.
        record = connection.execute(
            """
            SELECT * FROM outbox
            WHERE published_at IS NULL
            ORDER BY created_at, event_id
            LIMIT 1 FOR UPDATE SKIP LOCKED
            """
        ).fetchone()
        if record is None:
            return False
        publisher.publish(record)
        connection.execute(
            "UPDATE outbox SET published_at = clock_timestamp() WHERE event_id = %s",
            (record["event_id"],),
        )
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    publisher = KafkaPublisher()
    try:
        while True:
            try:
                with open_connection() as connection:
                    while publish_one(connection, publisher):
                        pass
                sleep(1)
            except (psycopg.Error, PublishError):
                logger.exception("Outbox publication failed; pending work will be retried")
                sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
