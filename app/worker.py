import logging
from time import sleep

import psycopg
from confluent_kafka import Consumer, KafkaException

from app.db import open_connection
from app.kafka import TOPIC, bootstrap_servers
from app.schemas import EventCreate

logger = logging.getLogger(__name__)


def create_consumer(servers: str | None = None, *, group_id: str, topic: str = TOPIC):
    consumer = Consumer(
        {
            "bootstrap.servers": servers or bootstrap_servers(),
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def process_record(consumer, record, handler) -> None:
    if record.error():
        raise KafkaException(record.error())
    event = EventCreate.model_validate_json(record.value())
    if record.key() != event.appointment_id.encode("utf-8"):
        raise ValueError("Kafka key does not match appointment_id")

    for attempt in range(3):
        try:
            with open_connection() as connection:
                handler(event, connection)
            committed = consumer.commit(message=record, asynchronous=False)
            for partition in committed or []:
                if partition.error:
                    raise KafkaException(partition.error)
            return
        except (psycopg.Error, KafkaException):
            if attempt == 2:
                raise
            logger.warning("Processing failed for event %s; retrying", event.event_id, exc_info=True)
            sleep(2**attempt)


def run(group_id, handler) -> None:
    logging.basicConfig(level=logging.INFO)
    consumer = create_consumer(group_id=group_id)
    try:
        while True:
            record = consumer.poll(1)
            if record is not None:
                process_record(consumer, record, handler)
    except KeyboardInterrupt:
        pass
    finally:
        # A failed record stops this worker, so later records cannot commit past it.
        consumer.close()


if __name__ == "__main__":
    from app.consumers.notifications import main

    main()
