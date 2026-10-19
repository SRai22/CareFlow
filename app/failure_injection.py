"""Consume one record, optionally terminating abruptly at a processing boundary."""

import argparse
import json
import os
import sys
from time import monotonic

from app.consumers import analytics, notifications
from app.kafka import TOPIC
from app.worker import create_consumer, process_record

CRASH_EXIT_CODE = 86
CRASH_POINTS = ("before_processing", "after_persist", "after_commit")


def report(outcome, point, record) -> None:
    print(json.dumps({
        "outcome": outcome,
        "checkpoint": point,
        "topic": record.topic(),
        "partition": record.partition(),
        "offset": record.offset(),
    }), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer", choices=("notifications", "analytics"), default="notifications")
    parser.add_argument("--group-id", required=True, help="Use a dedicated test consumer group")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--crash-at", choices=CRASH_POINTS)
    parser.add_argument("--timeout", type=float, default=30, help="Seconds to wait for a record")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    handler = {"notifications": notifications.process_event, "analytics": analytics.process_event}[
        args.consumer
    ]

    def checkpoint(point, record):
        if point == args.crash_at:
            report("crashed", point, record)
            # Bypass finally/close: this tests an abrupt loss of the consumer process.
            os._exit(CRASH_EXIT_CODE)

    # Short failure detection keeps local crash/restart exercises quick.
    consumer = create_consumer(group_id=args.group_id, topic=args.topic, session_timeout_ms=6000)
    try:
        deadline = monotonic() + args.timeout
        while monotonic() < deadline:
            record = consumer.poll(min(1, max(0, deadline - monotonic())))
            if record is not None:
                process_record(consumer, record, handler, checkpoint=checkpoint)
                report("processed", "after_commit", record)
                return 0
        print("Timed out waiting for a Kafka record; no offset was committed", file=sys.stderr)
        return 1
    finally:
        consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
