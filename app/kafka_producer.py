import json

from confluent_kafka import KafkaException, Producer

from app.kafka import TOPIC, bootstrap_servers


class PublishError(Exception):
    pass


class KafkaPublisher:
    def __init__(self, servers: str | None = None, topic: str = TOPIC):
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": servers or bootstrap_servers(),
                "enable.idempotence": True,
                "acks": "all",
                "partitioner": "murmur2_random",
                "message.timeout.ms": 5000,
                "allow.auto.create.topics": False,
            }
        )

    def publish(self, record: dict) -> None:
        delivery = []

        def delivered(error, message):
            delivery.append(error)

        try:
            self.producer.produce(
                self.topic,
                key=record["appointment_id"].encode("utf-8"),
                value=json.dumps(record["payload"], separators=(",", ":")).encode("utf-8"),
                on_delivery=delivered,
            )
            self.producer.flush(6)
        except (KafkaException, BufferError) as exc:
            raise PublishError("Kafka publication failed") from exc
        if not delivery or delivery[0] is not None:
            raise PublishError("Kafka did not acknowledge the event")
