# CareFlow

CareFlow receives appointment events, updates PostgreSQL state, and delivers
committed events through a transactional outbox to Kafka. Independent consumers
persist notification intents and analytics activity. This milestone sends no SMS
and calls no notification provider.

```text
POST /events → PostgreSQL transaction [event + appointment + outbox] → 201
                                      ↓
                              single outbox publisher
                                      ↓
                       appointments.events (3 partitions)
                           ↙                         ↘
             careflow-notifications          careflow-analytics
                notifications                  analytics_events
```

Python 3.12+ and Docker Compose are required. PostgreSQL and Kafka run in Docker;
the API, relay and consumers run locally.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
docker compose up -d --wait db kafka
docker compose run --rm kafka-init
export DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
python -m app.init_db
uvicorn app.main:app --reload
```

Run `app.init_db` **once on an empty database**. It creates all tables atomically
and fails if they exist. For an existing Phase 2A database, stop the old API and
`app.worker`, run this migration once, then start the updated processes:

```bash
python -m app.migrate_outbox
```

The migration adds the outbox and analytics tables without changing existing
appointments, successful HTTP outcomes or notification results. It deliberately
does not enqueue historical events: the previous direct producer may already
have published them. Retrying a historical HTTP request still replays its
original result; it does not repair gaps from the old direct-publication design.
The new outbox guarantee applies to events accepted after this upgrade.

If upgrading from an earlier schema, apply the missing migrations in order:
`app.migrate_idempotency`, `app.migrate_phase2`, then `app.migrate_outbox`.
Do not run a migration already applied. `migrate_idempotency` backfills request
hashes and successful responses; `migrate_phase2` adds persisted notification
decisions and the notification/provider tables. Historical notification decisions
remain null. Fresh databases already include all of these changes.

In three additional terminals, activate the environment and export the same
`DATABASE_URL` and `KAFKA_BOOTSTRAP_SERVERS`, then run one command per terminal:

```bash
python -m app.outbox_publisher
python -m app.consumers.notifications
python -m app.consumers.analytics
```

Run **one outbox publisher instance** for this milestone. `python -m app.worker`
is also an entry point for the intent-only notification consumer; it no longer
calls the mock provider. The earlier provider exercise remains in
`app/notifications.py` and `app/mock_provider.py`, but neither new consumer uses it.

Compose runs a single Kafka broker in KRaft mode, without ZooKeeper. The topic
initializer creates `appointments.events` with three partitions and replication
factor one. Host clients use `localhost:9092`; Compose-network clients can use
`kafka:29092`. Named volumes retain PostgreSQL data, Kafka records and committed
offsets across restarts. A one-shot storage initializer sets the Kafka volume's
ownership before its unprivileged broker starts. Credentials are for local
learning; use synthetic patient IDs.

API documentation: <http://127.0.0.1:8000/docs>.

```bash
curl -i http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "evt_001",
    "patient_id": "patient_123",
    "appointment_id": "appt_123",
    "event_type": "APPOINTMENT_SCHEDULED",
    "event_version": 1,
    "occurred_at": "2026-10-17T10:00:00Z"
  }'
curl http://127.0.0.1:8000/events/evt_001
curl http://127.0.0.1:8000/appointments/appt_123
```

## API and transaction contract

| Endpoint | Success | Errors |
| --- | --- | --- |
| `POST /events` | `201`, stored event plus `received_at`; new events are durably queued in the outbox | `409` conflicting event ID or existing appointment; `404` no matching appointment/patient on cancellation; `422` invalid payload; `500` unexpected database failure |
| `GET /events/{id}` | `200`, stored event plus `received_at` | `404` unknown ID; `422` invalid ID |
| `GET /appointments/{id}` | `200`, current appointment state | `404` unknown ID; `422` invalid ID |

A `201` confirms the PostgreSQL transaction, including the outbox insert. It does
not mean that Kafka or either consumer has completed processing. Kafka being
unavailable does not prevent accepting new events while PostgreSQL is available.
There is no longer a Kafka-related `503` from `POST /events`.

Pydantic rejects unknown fields and unsupported event types. Identifiers contain
1–128 ASCII letters, digits, underscores or hyphens. `event_version` is a positive
32-bit integer; it records the appointment event sequence, but sequence order is
not enforced yet. `occurred_at` requires a timezone and is normalized to UTC.

Each accepted event inserts its request fingerprint, stored successful HTTP
response, notification decision and immutable outbox payload in the same
transaction as the appointment change. The payload is a snapshot of the six
normalized request fields; publication never rebuilds it from current state.

Scheduling inserts a new `SCHEDULED` appointment; an existing appointment conflicts.
Cancellation locks an existing appointment matching both appointment and patient
IDs, then updates status, `last_event_id` and `updated_at`. No match raises
`LookupError` and rolls back the event and outbox. Matching patient IDs enforces
consistency; it is not authorization. Every database failure rolls back all
writes in that transaction.

Distinct repeated cancellations are accepted and each gets an event and outbox
row. Only a transition from `SCHEDULED` to `CANCELLED` warrants a cancellation
intent. Scheduling warrants a confirmation intent. These decisions are stored
on the event before commit, so a delayed consumer does not infer historical
transitions from later appointment state.

`event_id` is the request identity. SHA-256 of normalized, sorted JSON
fingerprints all six fields. The API attempts an insert first; PostgreSQL's
primary key resolves concurrent requests. Only `UniqueViolation` from
`events_pkey` triggers replay handling, **after rollback** of the failed
transaction. The same fingerprint returns the stored status/body and `Location`
without another state change or outbox row. Different content returns `409`.
`appointments_pkey` becomes `409`; unrelated database failures remain `500`.
No pre-insert existence check is used. Only successful requests retain outcomes.

## Outbox delivery and failure recovery

[app/outbox_publisher.py](app/outbox_publisher.py) repeatedly runs:

```sql
BEGIN;
SELECT * FROM outbox
WHERE published_at IS NULL
ORDER BY created_at, event_id
LIMIT 1 FOR UPDATE SKIP LOCKED;
-- Publish stored JSON, keyed by appointment_id; wait for broker acknowledgment.
UPDATE outbox SET published_at = clock_timestamp() WHERE event_id = ...;
COMMIT;
```

An empty queue waits one second before polling again. Database and publication
failures roll back, log the error and retry after one second. The worker opens a
new connection after an error, so a disconnected database can recover. Other
unexpected errors stop the worker with the row still pending.

[app/kafka_producer.py](app/kafka_producer.py) uses `enable.idempotence=true`,
`acks=all` and a stable appointment key. `produce()` only enqueues locally;
`flush()` serves the delivery callback, and publication succeeds only after a
successful callback. The message timeout is five seconds; flush waits up to six.
Errors and missing acknowledgments raise `PublishError`.
[Confluent producer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Producer.produce)

| Failure point | Recovery |
| --- | --- |
| Before the API transaction commits | Event, appointment change and outbox insert all roll back |
| After API commit, before relay publication | Pending row survives; relay publishes after restart |
| Before broker acknowledgment | Row stays pending; retry may produce a duplicate if acceptance was uncertain |
| After broker acknowledgment, before PostgreSQL commits the marker | Kafka record exists, row remains pending; restart publishes it again |
| After the published marker commits | Relay skips the row |

This is **at-least-once delivery**, without a distributed transaction. Kafka
producer idempotence covers its internal retries, not a new send after an outbox
crash. Consumer deduplication is therefore still required.

The prototype holds a row lock and database transaction during Kafka I/O. Slow
Kafka prolongs that transaction. `SKIP LOCKED` avoids competing for locked rows,
but multiple relay instances can publish later rows for the same appointment
first. **Per-appointment ordering across multiple publishers is not guaranteed.**
A Kafka key groups an appointment into one partition; it preserves broker append
order, not source sequence or PostgreSQL creation order. Parallel ordering,
claim leases and published-row retention are later milestones.

## Consumer groups, durable work and offsets

| Module | Group ID | Durable work |
| --- | --- | --- |
| `app.consumers.notifications` | `careflow-notifications` | Insert a unique `PENDING` notification intent when the stored business decision warrants one |
| `app.consumers.analytics` | `careflow-analytics` | Insert one `analytics_events` activity row per accepted event |

Separate groups each receive all topic records. Kafka distributes a group's
partitions among its members. Within each worker, records are processed
synchronously. Both consumers validate the payload and appointment key, and
compare its fingerprint with the committed event before writing anything.

The notification primary key `(event_id, notification_type)` and analytics
primary key `event_id` make repeated deliveries harmless. Existing `SENT` rows
from the previous exercise are preserved. This milestone only records intents:
no provider call or transition to `SENT` takes place.

Both `enable.auto.commit` and `enable.auto.offset.store` are disabled. After the
handler's PostgreSQL transaction commits, the worker synchronously commits
`record.offset + 1` as the next position to read and checks partition errors in
the commit result. A crash after the database commit and before the offset commit
replays the record; unique constraints prevent another durable outcome.
[Confluent commit API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit)

Database and Kafka commit errors receive three attempts, with waits of one and
two seconds. On exhaustion the worker exits nonzero and does not process later
records past the failed one. Invalid JSON, incorrect keys, mismatched payloads
and unexpected errors stop immediately. Closing the consumer does not commit.
Repair the cause and restart using the same group; pending records replay.
There is no dead-letter queue, provider sender or process supervisor yet.

## Consumer crash/restart harness

The recovery harness uses the real consumer handlers and forcibly exits its own
process at one selected checkpoint. `os._exit(86)` bypasses normal exception
handling and consumer cleanup, so this exercises an abrupt crash rather than a
graceful close. Normal consumers never enable these checkpoints.

Assuming a new event that warrants work and no earlier committed group offset:

| `--crash-at` | Durable work at crash | Committed offset at crash | Restart with the same group |
| --- | --- | --- | --- |
| `before_processing` | No work from this attempt | Unset | Read the same record and persist work |
| `after_persist` | One committed outcome | Unset | Read the same record; deduplication preserves the existing outcome |
| `after_commit` | One committed outcome | Record offset + 1 | Continue at the next record |

For a group with earlier progress, the first two cases leave its previous offset
unchanged. On replay, the handler validates the event identity and inserts with
`ON CONFLICT DO NOTHING`, then commits the offset after its transaction completes.
The unique deduplication identity and all work it represents must commit in the
same PostgreSQL transaction. A failure before commit must roll back all of that
work. Otherwise, an existing deduplication marker could hide unfinished work.
This guarantee covers the database intents/activity recorded here; external
notification delivery would require its own retry/idempotency contract.

With the same environment variables as the API, run:

```bash
python -m app.failure_injection --consumer notifications \
  --group-id careflow-chaos-notifications --crash-at after_persist
# Expected exit code: 86. Restart without --crash-at using the same group:
python -m app.failure_injection --consumer notifications \
  --group-id careflow-chaos-notifications
```

Inspect committed offsets and notification work separately:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group careflow-chaos-notifications
docker compose exec db psql -U careflow -d careflow -c 'TABLE notifications;'
```

Use `--consumer analytics` for the other handler. The harness prints a JSON
report containing the checkpoint, topic, partition and record offset. Without
`--crash-at`, it processes one record and exits successfully. `--topic` selects a
test topic; the default is `appointments.events`. Use a dedicated group so the
exercise does not advance either application's normal consumer group. New groups
start at the earliest available record, which may already have database work
from another group. Existing outcomes are preserved.

The harness uses a six-second consumer session timeout to shorten local failure
detection. Normal workers keep the client's 45-second default. A restart may
wait for the broker to detect the dead member and rebalance.
[Confluent consumer configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
The default record wait is 30 seconds; `--timeout` changes it. Waiting with no
record exits with code 1 and commits no offset. After `after_commit`, this is
expected if no later event exists; publish another event to observe forward
progress.

Run the automated recovery matrix with both test environment variables set:

```bash
pytest -q tests/test_failure_recovery.py
```

It crashes and restarts real subprocesses at all three points for both consumers,
using isolated PostgreSQL schemas, Kafka topics and groups. An independent
connection inspects durable work and broker-committed offsets after each crash.
A second event with the same appointment key proves whether restart replays the
first record or moves forward. Offset inspection uses an unsubscribed consumer
so observing progress does not join or rebalance the group. Tests also verify
that replay preserves the original outcome/timestamp, that uncommitted writes
roll back, and that no provider is called.

Broker outage injection, a DLQ policy, lag measurement and multi-consumer
assignment tests remain the next recovery exercises. Poison records still stop
the current worker under the existing explicit error policy.

## Tables

See [app/schema.sql](app/schema.sql),
[app/notification_schema.sql](app/notification_schema.sql) and
[app/messaging_schema.sql](app/messaging_schema.sql).

| Table | Purpose |
| --- | --- |
| `events` | Accepted immutable request fields, successful responses and notification decisions; primary key `event_id` |
| `appointments` | Current state; primary key `appointment_id` |
| `outbox` | Immutable publication snapshots and `published_at`; primary key `event_id`, partial pending index on `(created_at, event_id)` |
| `notifications` | Unique event/type intents; retains previous provider outcome fields |
| `analytics_events` | Deduplicated activity with appointment, event type, source time and processing time |
| `mock_provider_acceptances` | Retained storage from the previous mock-provider exercise; unused by these consumers |

`idx_events_appointment_id` indexes appointment event-history lookups. Primary
keys also create indexes. There is no patient demographics table.

## Verify

With both services running and `appointments.events` initialized:

```bash
export TEST_DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export TEST_KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
pytest -q
```

Each database test creates and removes its own temporary schema. Broker tests
create isolated three-partition topics and consumer groups; application records
are untouched. The test database role needs schema creation permission. Database
tests require real PostgreSQL. Broker tests explicitly skip if their environment
variable is unset; set both variables for the full suite.

Tests cover API validation, patient matching, atomic rollback, HTTP identity and
concurrent retries, one outbox row per accepted event, rejected events leaving no
outbox row, immutable scheduling payloads after cancellation, failed publication,
crashed relay lock recovery, competing row claims, and actual Kafka duplication
after acknowledgment followed by database failure. Consumer tests cover separate
groups, independent appointments, duplicate deliveries, restart before and after
offset commits, three-attempt database failure, invalid records, and repeated
cancellations that require no second notification intent.

`tests/test_kafka.py::test_api_outbox_kafka_key_and_http_replay` implements the first
acceptance check: POST `appt_123`, inspect persisted state, relay the row, verify
its Kafka key and payload, retry HTTP, then verify there is no additional record.

To inspect the development topic and durable outcomes:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic appointments.events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic appointments.events \
  --from-beginning --property print.key=true
docker compose exec db psql -U careflow -d careflow -c \
  'SELECT event_id, appointment_id, published_at FROM outbox ORDER BY created_at;'
docker compose exec db psql -U careflow -d careflow -c 'TABLE notifications;'
docker compose exec db psql -U careflow -d careflow -c 'TABLE analytics_events;'
```

Source event ordering, cancellation-before-scheduling recovery, authorization,
connection pooling, burst control, Temporal, Redis and production observability
remain outside this milestone.
