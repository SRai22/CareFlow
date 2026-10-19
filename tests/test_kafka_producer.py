import json
import pytest
from confluent_kafka import KafkaError, KafkaException

from app import kafka_producer
from app.kafka_producer import KafkaPublisher, PublishError


@pytest.mark.parametrize("outcome", ["ack", "delivery_error", "no_ack", "buffer_full", "kafka_error"])
def test_producer_requires_ack_and_uses_outbox_snapshot(monkeypatch, outcome):
    observed = {}

    class Producer:
        def __init__(self, configuration):
            observed["configuration"] = configuration

        def produce(self, topic, **kwargs):
            observed.update(topic=topic, **kwargs)
            if outcome == "buffer_full":
                raise BufferError("full")
            if outcome == "kafka_error":
                raise KafkaException(KafkaError(KafkaError._TRANSPORT))

        def flush(self, timeout):
            observed["timeout"] = timeout
            if outcome != "no_ack":
                observed["on_delivery"](None if outcome == "ack" else "rejected", None)
            return int(outcome == "no_ack")

    monkeypatch.setattr(kafka_producer, "Producer", Producer)
    publisher = KafkaPublisher("broker:9092")
    row = {"appointment_id": "appt_123", "payload": {"event_id": "evt_123", "event_type": "APPOINTMENT_SCHEDULED"}}
    if outcome == "ack":
        publisher.publish(row)
        assert observed["key"] == b"appt_123"
        assert json.loads(observed["value"]) == row["payload"]
        assert observed["topic"] == "appointments.events"
    else:
        with pytest.raises(PublishError):
            publisher.publish(row)
    assert observed["configuration"]["enable.idempotence"] is True
    assert observed["configuration"]["acks"] == "all"
