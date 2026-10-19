import os

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from app.workflows.appointment_reminder import AppointmentReminderWorkflow, ReminderInput

TASK_QUEUE = "careflow-reminders"


def workflow_id(appointment_id: str) -> str:
    return f"appointment-reminder:{appointment_id}"


async def connect_client() -> Client:
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )


async def start_reminder(client, appointment_id: str, delay_seconds: float, *, task_queue=TASK_QUEUE):
    return await client.start_workflow(
        AppointmentReminderWorkflow.run,
        ReminderInput(appointment_id, delay_seconds),
        id=workflow_id(appointment_id),
        task_queue=task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )
