import os
from datetime import datetime, timezone

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from app.workflows.appointment_reminder import AppointmentReminderWorkflow, ReminderInput

TASK_QUEUE = "careflow-reminders"


class TemporalHandoffError(Exception):
    pass


class ReminderConflict(ValueError):
    pass


def workflow_id(appointment_id: str, schedule_generation: int | None = None) -> str:
    base = f"appointment-reminder:{appointment_id}"
    return f"{base}:g{schedule_generation}" if schedule_generation and schedule_generation > 1 else base


async def connect_client() -> Client:
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )


def reminder_input(appointment_id: str, scheduled_for: datetime, schedule_generation=None, event_id=None) -> ReminderInput:
    if scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None:
        raise ValueError("scheduled_for must include a timezone")
    return ReminderInput(appointment_id, scheduled_for=scheduled_for.astimezone(timezone.utc).isoformat(),
                         schedule_generation=schedule_generation, event_id=event_id)


def reminder_queue() -> str:
    return os.environ.get("TEMPORAL_TASK_QUEUE", TASK_QUEUE)


async def start_reminder(client, appointment_id: str, scheduled_for: datetime, *, task_queue=None, schedule_generation=None, event_id=None):
    return await client.start_workflow(
        AppointmentReminderWorkflow.run,
        reminder_input(appointment_id, scheduled_for, schedule_generation, event_id),
        id=workflow_id(appointment_id, schedule_generation),
        task_queue=task_queue or reminder_queue(),
        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )


async def ensure_reminder(client, appointment_id: str, scheduled_for: datetime, *, task_queue=None, schedule_generation=None, event_id=None, allow_closed=False):
    queue = task_queue or reminder_queue()
    try:
        return await start_reminder(client, appointment_id, scheduled_for, task_queue=queue,
                                    schedule_generation=schedule_generation, event_id=event_id)
    except WorkflowAlreadyStartedError as exc:
        handle = client.get_workflow_handle(workflow_id(appointment_id, schedule_generation), run_id=exc.run_id)
        description = await handle.describe()
        history = await handle.fetch_history()
        started = history.events[0].workflow_execution_started_event_attributes
        expected = reminder_input(appointment_id, scheduled_for, schedule_generation, event_id)
        if (
            started.workflow_type.name != "AppointmentReminderWorkflow"
            or started.task_queue.name != queue
            or await client.data_converter.decode(started.input.payloads, [ReminderInput]) != [expected]
            or (not allow_closed and description.status not in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.COMPLETED))
        ):
            raise ReminderConflict("Existing workflow does not match the desired appointment reminder") from exc
        return handle



async def cancel_reminder(client, source):
    handle = await ensure_reminder(
        client, source["appointment_id"], source["scheduled_for"],
        schedule_generation=source["schedule_generation"],
        event_id=source["event_id"] if source["schedule_generation"] is not None else None,
        allow_closed=True,
    )
    if (await handle.describe()).status != WorkflowExecutionStatus.RUNNING:
        return "ALREADY_CLOSED"
    try:
        await handle.signal(AppointmentReminderWorkflow.cancel)
        return "SIGNALLED"
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND and (await handle.describe()).status != WorkflowExecutionStatus.RUNNING:
            return "ALREADY_CLOSED"
        raise
