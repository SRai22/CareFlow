from dataclasses import dataclass

from temporalio import workflow


@dataclass
class ReminderInput:
    appointment_id: str
    delay_seconds: float


@workflow.defn
class AppointmentReminderWorkflow:
    @workflow.run
    async def run(self, request: ReminderInput) -> str:
        # Milestone 4.1 proves recovery; appointment timing and delivery come later.
        await workflow.sleep(request.delay_seconds)
        return f"Reminder workflow completed for {request.appointment_id}"
