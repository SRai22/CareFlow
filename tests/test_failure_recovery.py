import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from confluent_kafka import OFFSET_INVALID, TopicPartition
import pytest

from app.consumers import analytics, notifications
from app.failure_injection import CRASH_EXIT_CODE
from app.outbox_publisher import publish_one
from app.schemas import EventCreate


@pytest.fixture
def run_consumer(database_url, kafka):
    def run(name, group, crash_at=None, wait_seconds=20):
        command = [
            sys.executable, "-m", "app.failure_injection", "--consumer", name,
            "--group-id", group, "--topic", kafka.topic, "--timeout", str(wait_seconds),
        ]
        if crash_at:
            command.extend(["--crash-at", crash_at])
        return subprocess.run(
            command, capture_output=True, text=True, timeout=35,
            cwd=Path(__file__).parents[1],
            env={
                **os.environ, "DATABASE_URL": database_url,
                "KAFKA_BOOTSTRAP_SERVERS": os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"],
            },
        )
    return run


def test_empty_topic_times_out_without_work_or_offset_commit(connection, kafka, run_consumer):
    group = f"empty-{uuid4().hex}"
    result = run_consumer("notifications", group, wait_seconds=1)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Timed out" in result.stderr
    assert connection.execute("SELECT count(*) FROM notifications").fetchone()["count"] == 0
    observer = kafka.observer(group)
    offsets = observer.committed([TopicPartition(kafka.topic, p) for p in range(3)], timeout=5)
    assert all(position.error is None and position.offset == OFFSET_INVALID for position in offsets)


@pytest.mark.parametrize("name,table", [
    ("notifications", "notifications"), ("analytics", "analytics_events"),
])
@pytest.mark.parametrize("crash_at", ["before_processing", "after_persist", "after_commit"])
def test_process_crash_then_restart_recovers_state_and_offset(
    client, event, connection, kafka, run_consumer, name, table, crash_at
):
    assert client.post("/events", json=event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    original = kafka.read(kafka.reader())
    group = f"crash-{name}-{uuid4().hex}"
    observer = kafka.observer(group)

    crashed = run_consumer(name, group, crash_at)
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stdout + crashed.stderr
    report = json.loads(crashed.stdout)
    assert report == {
        "outcome": "crashed", "checkpoint": crash_at,
        "topic": original.topic(), "partition": original.partition(), "offset": original.offset(),
    }
    persisted = connection.execute(f"SELECT * FROM {table} ORDER BY event_id").fetchall()
    assert len(persisted) == (0 if crash_at == "before_processing" else 1)
    expected_offset = original.offset() + 1 if crash_at == "after_commit" else OFFSET_INVALID
    assert kafka.offset(observer, original) == expected_offset

    # Same key puts the marker in the same partition: it proves the restart position.
    next_event = {**event, "event_id": "evt_next", "event_type": "APPOINTMENT_CANCELLED", "event_version": 2}
    assert client.post("/events", json=next_event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    recovered = run_consumer(name, group)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    recovered_report = json.loads(recovered.stdout)
    resumed_offset = original.offset() + int(crash_at == "after_commit")
    assert recovered_report == {
        "outcome": "processed", "checkpoint": "after_commit",
        "topic": original.topic(), "partition": original.partition(), "offset": resumed_offset,
    }
    assert kafka.offset(observer, original) == resumed_offset + 1
    outcomes = connection.execute(f"SELECT * FROM {table} ORDER BY event_id").fetchall()
    assert len(outcomes) == (2 if crash_at == "after_commit" else 1)
    if persisted:
        assert outcomes[0] == persisted[0]  # Replay must preserve the original work and timestamp.
    if crash_at != "after_commit":
        assert outcomes[0]["event_id"] == event["event_id"]
        following = run_consumer(name, group)
        assert following.returncode == 0, following.stdout + following.stderr
        assert json.loads(following.stdout)["offset"] == original.offset() + 1
    assert kafka.offset(observer, original) == original.offset() + 2
    assert {row["event_id"] for row in connection.execute(f"SELECT * FROM {table}").fetchall()} == {
        event["event_id"], next_event["event_id"],
    }
    assert connection.execute("SELECT count(*) FROM mock_provider_acceptances").fetchone()["count"] == 0


@pytest.mark.parametrize("handler,table", [
    (notifications.process_event, "notifications"), (analytics.process_event, "analytics_events"),
])
def test_uncommitted_work_rolls_back_before_retry(client, event, connection, handler, table):
    assert client.post("/events", json=event).status_code == 201
    with pytest.raises(RuntimeError, match="before commit"):
        with connection.transaction():
            handler(EventCreate(**event), connection)
            raise RuntimeError("failed before commit")
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 0
    handler(EventCreate(**event), connection)
    handler(EventCreate(**event), connection)
    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()["count"] == 1
