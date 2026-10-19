from dataclasses import dataclass
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError


@dataclass
class ReminderInput:
    appointment_id: str
    delay_seconds: float | None = None
    scheduled_for: str | None = None
    schedule_generation: int | None = None
    event_id: str | None = None


@workflow.defn
class AppointmentReminderWorkflow:
    def __init__(self):
        self.cancelled = False

    @workflow.signal
    def cancel(self) -> None:
        self.cancelled = True

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
        if request.schedule_generation is None:
            # Existing 4.2 timer demos keep their original command history and have no side effects.
            if delay:
                await workflow.sleep(delay)
            return {"appointment_id": request.appointment_id, "status": "REMINDER_DUE"}
        if delay and not self.cancelled:
            try:
                await workflow.wait_condition(lambda: self.cancelled, timeout=delay)
            except TimeoutError:
                pass
        if self.cancelled:
            return {"appointment_id": request.appointment_id, "status": "CANCELLED"}
        # Once dispatched, cancellation does not cancel an external request already authorized.
        try:
            return await workflow.execute_activity(
                "deliver_reminder", request, result_type=dict,
                start_to_close_timeout=timedelta(seconds=15),
                schedule_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2,
                                         maximum_interval=timedelta(seconds=30), maximum_attempts=5),
            )
        except ActivityError:
            return await workflow.execute_activity(
                "finish_uncertain_delivery", request, result_type=dict,
                start_to_close_timeout=timedelta(seconds=15),
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
