import os
from functools import lru_cache

from confluent_kafka import KafkaException, Producer

from app.schemas import EventCreate

TOPIC = "appointments.events"


class PublishError(Exception):
    pass


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class KafkaPublisher:
    def __init__(self, servers: str, topic: str = TOPIC):
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": servers,
                "enable.idempotence": True,
                "acks": "all",
                "partitioner": "murmur2_random",
                "message.timeout.ms": 5000,
                "allow.auto.create.topics": False,
            }
        )

    def publish(self, event: EventCreate) -> None:
        delivery = []

        def delivered(error, message):
            delivery.append(error)

        try:
            self.producer.produce(
                self.topic,
                key=event.appointment_id.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
                on_delivery=delivered,
            )
            self.producer.flush(6)
        except (KafkaException, BufferError) as exc:
            raise PublishError("Kafka publication failed") from exc
        if not delivery or delivery[0] is not None:
            raise PublishError("Kafka did not acknowledge the event")


@lru_cache
def get_publisher() -> KafkaPublisher:
    return KafkaPublisher(bootstrap_servers())
