from dataclasses import dataclass
from datetime import datetime, timedelta

from temporalio import workflow


@dataclass
class ReminderInput:
    appointment_id: str
    delay_seconds: float | None = None
    scheduled_for: str | None = None


@workflow.defn
class AppointmentReminderWorkflow:
    @workflow.run
    async def run(self, request: ReminderInput) -> dict | str:
        if request.scheduled_for is None:
            # Preserve replay of the input and commands of in-flight Milestone 4.1 demos.
            if request.delay_seconds is None:
                raise ValueError("scheduled_for is required")
            await workflow.sleep(request.delay_seconds)
            return f"Reminder workflow completed for {request.appointment_id}"
        reminder_time = datetime.fromisoformat(request.scheduled_for) - timedelta(hours=24)
        delay = max(0, (reminder_time - workflow.now()).total_seconds())
        if delay:
            await workflow.sleep(delay)
        return {"appointment_id": request.appointment_id, "status": "REMINDER_DUE"}
