import asyncio
from contextlib import asynccontextmanager
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
            "--appointment-id", appointment_id, "--delay-seconds", "0.1", "--task-queue", queue,
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
                    assert await handle.result() == f"Reminder workflow completed for {appointment_id}"
                assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
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
        handle = await start_reminder(client, appointment_id, 3, task_queue=queue)
        original_run_id = (await handle.describe()).run_id
        try:
            with pytest.raises(WorkflowAlreadyStartedError):
                await start_reminder(client, appointment_id, 3, task_queue=queue)
            async with worker_process(temporal_settings, queue) as worker:
                history = await wait_for_event(handle, EventType.EVENT_TYPE_TIMER_STARTED)
                assert not any(e.event_type == EventType.EVENT_TYPE_TIMER_FIRED for e in history.events)
                worker.kill()
                await worker.wait()
                assert worker.returncode == -signal.SIGKILL

            # The timer fires in Temporal while every worker for this queue is down.
            await wait_for_event(handle, EventType.EVENT_TYPE_TIMER_FIRED)
            client = await Client.connect(address, namespace=namespace)
            recovered = client.get_workflow_handle(handle.id, run_id=original_run_id)
            assert (await recovered.describe()).status == WorkflowExecutionStatus.RUNNING
            assert (await recovered.describe()).run_id == original_run_id
            async with worker_process(temporal_settings, queue):
                assert await recovered.result() == f"Reminder workflow completed for {appointment_id}"

            assert (await recovered.describe()).status == WorkflowExecutionStatus.COMPLETED
            history = await recovered.fetch_history()
            for event_type in (
                EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventType.EVENT_TYPE_TIMER_STARTED,
                EventType.EVENT_TYPE_TIMER_FIRED, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            ):
                assert sum(e.event_type == event_type for e in history.events) == 1
            with pytest.raises(WorkflowAlreadyStartedError):
                await start_reminder(client, appointment_id, 3, task_queue=queue)
        finally:
            await clean_up(handle)
    asyncio.run(asyncio.wait_for(scenario(), timeout=60))
