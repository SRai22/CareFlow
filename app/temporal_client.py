import os
from datetime import datetime, timezone

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.workflows.appointment_reminder import AppointmentReminderWorkflow, ReminderInput

TASK_QUEUE = "careflow-reminders"


class TemporalHandoffError(Exception):
    pass


class ReminderConflict(ValueError):
    pass


def workflow_id(appointment_id: str) -> str:
    return f"appointment-reminder:{appointment_id}"


async def connect_client() -> Client:
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )


def reminder_input(appointment_id: str, scheduled_for: datetime) -> ReminderInput:
    if scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None:
        raise ValueError("scheduled_for must include a timezone")
    return ReminderInput(appointment_id, scheduled_for=scheduled_for.astimezone(timezone.utc).isoformat())


def reminder_queue() -> str:
    return os.environ.get("TEMPORAL_TASK_QUEUE", TASK_QUEUE)


async def start_reminder(client, appointment_id: str, scheduled_for: datetime, *, task_queue=None):
    return await client.start_workflow(
        AppointmentReminderWorkflow.run,
        reminder_input(appointment_id, scheduled_for),
        id=workflow_id(appointment_id),
        task_queue=task_queue or reminder_queue(),
        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )


async def ensure_reminder(client, appointment_id: str, scheduled_for: datetime, *, task_queue=None):
    queue = task_queue or reminder_queue()
    try:
        return await start_reminder(client, appointment_id, scheduled_for, task_queue=queue)
    except WorkflowAlreadyStartedError as exc:
        handle = client.get_workflow_handle(workflow_id(appointment_id), run_id=exc.run_id)
        description = await handle.describe()
        history = await handle.fetch_history()
        started = history.events[0].workflow_execution_started_event_attributes
        expected = reminder_input(appointment_id, scheduled_for)
        if (
            started.workflow_type.name != "AppointmentReminderWorkflow"
            or started.task_queue.name != queue
            or await client.data_converter.decode(started.input.payloads, [ReminderInput]) != [expected]
            or description.status not in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.COMPLETED)
        ):
            raise ReminderConflict("Existing workflow does not match the desired appointment reminder") from exc
        return handle
