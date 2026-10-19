import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import sys
from uuid import uuid4

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.exceptions import WorkflowAlreadyStartedError
from confluent_kafka import OFFSET_INVALID

from app import worker
from app.consumers import reminders
from app.outbox_publisher import publish_one
from app.temporal_client import ReminderConflict, TemporalHandoffError, ensure_reminder
from app.workflows.appointment_reminder import AppointmentReminderWorkflow

from app.temporal_client import start_reminder, workflow_id


@pytest.fixture
def temporal_settings():
    address = os.environ.get("TEST_TEMPORAL_ADDRESS")
    if not address:
        pytest.skip("Set TEST_TEMPORAL_ADDRESS to run real Temporal tests")
    return address, os.environ.get("TEST_TEMPORAL_NAMESPACE", "default")


@asynccontextmanager
async def worker_process(settings, task_queue):
    address, namespace = settings
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "app.temporal_worker", "--task-queue", task_queue,
        cwd=Path(__file__).parents[1],
        env={**os.environ, "TEMPORAL_ADDRESS": address, "TEMPORAL_NAMESPACE": namespace},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    try:
        async def wait_until_ready():
            output = []
            while True:
                line = await process.stdout.readline()
                assert line, b"".join(output).decode()
                output.append(line)
                try:
                    report = json.loads(line)
                except ValueError:
                    continue
                if report == {"status": "ready", "task_queue": task_queue}:
                    return
        await asyncio.wait_for(wait_until_ready(), timeout=20)
        yield process
    finally:
        if process.returncode is None:
            process.send_signal(signal.SIGINT)
            try:
                await asyncio.wait_for(process.wait(), timeout=10)
            except TimeoutError:
                process.kill()
                await process.wait()


async def wait_for_event(handle, event_type):
    async def poll_history():
        while True:
            history = await handle.fetch_history()
            if any(event.event_type == event_type for event in history.events):
                return history
            await asyncio.sleep(0.05)
    return await asyncio.wait_for(poll_history(), timeout=20)


async def clean_up(handle):
    if (await handle.describe()).status == WorkflowExecutionStatus.RUNNING:
        await handle.terminate("Temporal test cleanup")


def test_cli_starts_durable_execution_before_worker_exists(temporal_settings):
    async def scenario():
        address, namespace = temporal_settings
        appointment_id = f"appt_temporal_{uuid4().hex}"
        queue = f"careflow-reminders-test-{uuid4().hex}"
        client = await Client.connect(address, namespace=namespace)
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "app.start_reminder",
            "--appointment-id", appointment_id, "--scheduled-for", (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), "--task-queue", queue,
            cwd=Path(__file__).parents[1],
            env={**os.environ, "TEMPORAL_ADDRESS": address, "TEMPORAL_NAMESPACE": namespace},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
            assert process.returncode == 0, stderr.decode()
            report = json.loads(stdout)
            assert report["workflow_id"] == workflow_id(appointment_id)
            # A new client can recover the execution even though no worker has polled yet.
            client = await Client.connect(address, namespace=namespace)
            handle = client.get_workflow_handle(report["workflow_id"], run_id=report["run_id"])
            try:
                assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
                async with worker_process(temporal_settings, queue):
                    assert await handle.result() == {"appointment_id": appointment_id, "status": "REMINDER_DUE"}
                assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
                history = await handle.fetch_history()
                assert not any(e.event_type == EventType.EVENT_TYPE_TIMER_STARTED for e in history.events)
            finally:
                await clean_up(handle)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
    asyncio.run(asyncio.wait_for(scenario(), timeout=60))


def test_worker_crash_preserves_timer_and_same_execution(temporal_settings):
    async def scenario():
        address, namespace = temporal_settings
        client = await Client.connect(address, namespace=namespace)
        appointment_id = f"appt_temporal_{uuid4().hex}"
        queue = f"careflow-reminders-test-{uuid4().hex}"
        scheduled_for = datetime.now(timezone.utc) + timedelta(days=1, seconds=3)
        handle = await start_reminder(client, appointment_id, scheduled_for, task_queue=queue)
        original_run_id = (await handle.describe()).run_id
        try:
            with pytest.raises(WorkflowAlreadyStartedError):
                await start_reminder(client, appointment_id, scheduled_for, task_queue=queue)
            async with worker_process(temporal_settings, queue) as worker:
                history = await wait_for_event(handle, EventType.EVENT_TYPE_TIMER_STARTED)
                started = next(e for e in history.events if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED)
                task_started = next(e for e in history.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED)
                deadline = task_started.event_time.ToDatetime(tzinfo=timezone.utc) + started.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
                assert abs((deadline - (scheduled_for - timedelta(hours=24))).total_seconds()) < 0.05
                assert not any(e.event_type == EventType.EVENT_TYPE_TIMER_FIRED for e in history.events)
                worker.kill()
                await worker.wait()
                assert worker.returncode == -signal.SIGKILL

            # The timer fires in Temporal while every worker for this queue is down.
            fired_history = await wait_for_event(handle, EventType.EVENT_TYPE_TIMER_FIRED)
            fired = next(e for e in fired_history.events if e.event_type == EventType.EVENT_TYPE_TIMER_FIRED)
            assert fired.event_time.ToDatetime(tzinfo=timezone.utc) >= scheduled_for - timedelta(hours=24)
            client = await Client.connect(address, namespace=namespace)
            recovered = client.get_workflow_handle(handle.id, run_id=original_run_id)
            assert (await recovered.describe()).status == WorkflowExecutionStatus.RUNNING
            assert (await recovered.describe()).run_id == original_run_id
            async with worker_process(temporal_settings, queue):
                assert await recovered.result() == {"appointment_id": appointment_id, "status": "REMINDER_DUE"}

            assert (await recovered.describe()).status == WorkflowExecutionStatus.COMPLETED
            history = await recovered.fetch_history()
            for event_type in (
                EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventType.EVENT_TYPE_TIMER_STARTED,
                EventType.EVENT_TYPE_TIMER_FIRED, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            ):
                assert sum(e.event_type == event_type for e in history.events) == 1
            with pytest.raises(WorkflowAlreadyStartedError):
                await start_reminder(client, appointment_id, scheduled_for, task_queue=queue)
            duplicate = await ensure_reminder(client, appointment_id, scheduled_for, task_queue=queue)
            assert (await duplicate.describe()).run_id == original_run_id
        finally:
            await clean_up(handle)
    asyncio.run(asyncio.wait_for(scenario(), timeout=60))


@pytest.fixture
def reminder_event(event):
    return {**event, "appointment_id": f"appt_handoff_{uuid4().hex}"}


@pytest.fixture
def handoff_settings(temporal_settings, monkeypatch):
    address, namespace = temporal_settings
    queue = f"careflow-reminders-test-{uuid4().hex}"
    monkeypatch.setenv("TEMPORAL_ADDRESS", address)
    monkeypatch.setenv("TEMPORAL_NAMESPACE", namespace)
    monkeypatch.setenv("TEMPORAL_TASK_QUEUE", queue)
    return temporal_settings, queue


def test_kafka_handoff_duplicates_and_restart_preserve_one_execution(
    client, reminder_event, connection, kafka, handoff_settings
):
    assert client.post("/events", json=reminder_event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    kafka.publisher.publish(connection.execute("SELECT * FROM outbox").fetchone())
    group = f"{reminders.GROUP_ID}-{uuid4().hex}"
    consumer = kafka.consumer(group)
    first = kafka.read(consumer)

    def crash(point, record):
        if point == "after_persist":
            raise RuntimeError("Crash after Temporal acknowledgment, before offset commit")

    with pytest.raises(RuntimeError, match="after Temporal"):
        worker.process_record(consumer, first, reminders.process_event, checkpoint=crash)
    assert kafka.offset(consumer, first) == OFFSET_INVALID

    async def scenario():
        settings, queue = handoff_settings
        temporal = await Client.connect(settings[0], namespace=settings[1])
        handle = temporal.get_workflow_handle(workflow_id(reminder_event["appointment_id"]))
        try:
            original_run = (await handle.describe()).run_id
            assert handle.id == f'appointment-reminder:{reminder_event["appointment_id"]}'
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            # Recreate the Kafka consumer with the same group; the uncommitted record reappears.
            kafka.close(consumer)
            restarted = kafka.consumer(group)
            replay = kafka.read(restarted)
            assert (replay.partition(), replay.offset()) == (first.partition(), first.offset())
            await asyncio.to_thread(worker.process_record, restarted, replay, reminders.process_event)
            assert kafka.offset(restarted, replay) == replay.offset() + 1
            duplicate = kafka.read(restarted)
            await asyncio.to_thread(worker.process_record, restarted, duplicate, reminders.process_event)
            assert kafka.offset(restarted, duplicate) == duplicate.offset() + 1
            assert (await handle.describe()).run_id == original_run
            history = await handle.fetch_history()
            assert sum(e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED for e in history.events) == 1
        finally:
            await clean_up(handle)
    asyncio.run(asyncio.wait_for(scenario(), timeout=45))


def test_temporal_unavailable_retains_offset_and_recovers_on_consumer_restart(
    client, reminder_event, connection, kafka, handoff_settings, monkeypatch
):
    assert client.post("/events", json=reminder_event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    group = f"unavailable-{uuid4().hex}"
    consumer = kafka.consumer(group)
    record = kafka.read(consumer)
    with monkeypatch.context() as patch:
        patch.setenv("TEMPORAL_ADDRESS", "127.0.0.1:1")
        patch.setattr(reminders, "HANDOFF_TIMEOUT_SECONDS", 0.2)
        sleeps = []
        patch.setattr(worker, "sleep", sleeps.append)
        with pytest.raises(TemporalHandoffError):
            worker.process_record(consumer, record, reminders.process_event)
        assert sleeps == [1, 2]
    assert kafka.offset(consumer, record) == OFFSET_INVALID
    kafka.close(consumer)
    restarted = kafka.consumer(group)
    replay = kafka.read(restarted)
    assert (replay.partition(), replay.offset()) == (record.partition(), record.offset())
    worker.process_record(restarted, replay, reminders.process_event)
    assert kafka.offset(restarted, replay) == replay.offset() + 1

    async def verify():
        settings, _ = handoff_settings
        temporal = await Client.connect(settings[0], namespace=settings[1])
        handle = temporal.get_workflow_handle(workflow_id(reminder_event["appointment_id"]))
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        await clean_up(handle)
    asyncio.run(verify())


@pytest.mark.parametrize("conflict", ["time", "terminated", "legacy"])
def test_existing_workflow_must_match_desired_reminder_before_offset_commit(
    client, reminder_event, connection, kafka, handoff_settings, conflict
):
    assert client.post("/events", json=reminder_event).status_code == 201
    assert publish_one(connection, kafka.publisher)
    consumer = kafka.consumer()
    record = kafka.read(consumer)

    async def scenario():
        settings, queue = handoff_settings
        temporal = await Client.connect(settings[0], namespace=settings[1])
        appointment_id = reminder_event["appointment_id"]
        scheduled_for = datetime.fromisoformat(reminder_event["scheduled_for"])
        if conflict == "legacy":
            handle = await temporal.start_workflow(
                AppointmentReminderWorkflow.run,
                {"appointment_id": appointment_id, "delay_seconds": 30},
                id=workflow_id(appointment_id), task_queue=queue,
            )
        else:
            handle = await start_reminder(
                temporal, appointment_id, scheduled_for + timedelta(hours=int(conflict == "time")),
                task_queue=queue,
            )
        try:
            if conflict == "terminated":
                await handle.terminate("Injected failed reminder")
            with pytest.raises(ReminderConflict):
                await asyncio.to_thread(worker.process_record, consumer, record, reminders.process_event)
            assert kafka.offset(consumer, record) == OFFSET_INVALID
        finally:
            await clean_up(handle)
    asyncio.run(asyncio.wait_for(scenario(), timeout=30))


def test_inflight_milestone41_input_still_replays(temporal_settings):
    async def scenario():
        address, namespace = temporal_settings
        temporal = await Client.connect(address, namespace=namespace)
        appointment_id = f"appt_legacy_{uuid4().hex}"
        queue = f"careflow-legacy-test-{uuid4().hex}"
        handle = await temporal.start_workflow(
            AppointmentReminderWorkflow.run,
            {"appointment_id": appointment_id, "delay_seconds": 0.1},
            id=workflow_id(appointment_id), task_queue=queue,
        )
        try:
            async with worker_process(temporal_settings, queue):
                assert await handle.result() == f"Reminder workflow completed for {appointment_id}"
        finally:
            await clean_up(handle)
    asyncio.run(asyncio.wait_for(scenario(), timeout=30))
