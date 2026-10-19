# CareFlow

CareFlow receives appointment events, updates PostgreSQL state, and delivers
committed events through a transactional outbox to Kafka. Independent consumers
persist notification intents and analytics activity. Temporal reminder activities
call a durable local fake provider; no real SMS is sent.

Milestones 4.3–4.4 add cancellation signals, schedule generations, and retryable
reminder delivery. A generation waits until `scheduled_for - 24 hours`, then a
PostgreSQL dispatch claim authorizes one logical provider request.

```text
POST /events → PostgreSQL transaction [event + appointment + outbox] → 201
                                      ↓
                              single outbox publisher
                                      ↓
                       appointments.events (3 partitions)
             ├─ careflow-notifications → notification intents
             ├─ careflow-analytics → event activity
             └─ careflow-reminders → Temporal timer/signal → dispatch claim → fake provider
```

Python 3.12+ and Docker Compose are required. PostgreSQL, Kafka and Temporal run
in Docker; the API, relay, consumers and Temporal worker run locally.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
docker compose up -d --wait db kafka temporal
docker compose run --rm kafka-init
export DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export TEMPORAL_ADDRESS='localhost:7233'
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

For an existing Milestone 4.1 database, stop the API and workers, then run once:

```bash
python -m app.migrate_scheduled_for
```

For an existing Milestone 4.2 database, stop the API and workers and run once:

```bash
python -m app.migrate_reminder_delivery
```

This migration adds schedule generations, related delivery records and durable
Kafka handoff receipts. Existing notification outcomes, request hashes and
outbox payloads are preserved. Existing appointments start at generation 1;
legacy event generation remains null to identify the earlier timer-only protocol.

Restart the API and workers using the updated code. The appointment-time migration adds
nullable `scheduled_for TIMESTAMPTZ` columns to appointments and events. Existing
rows remain readable, with unknown times left null. Existing request hashes,
stored HTTP responses and outbox payloads are preserved. Historical events are
not assigned invented times or automatically enqueued for reminders.

If upgrading from an earlier schema, apply the missing migrations in order:
`app.migrate_idempotency`, `app.migrate_phase2`, `app.migrate_outbox`, then
`app.migrate_scheduled_for`, then `app.migrate_reminder_delivery`.
Do not run a migration already applied. `migrate_idempotency` backfills request
hashes and successful responses; `migrate_phase2` adds persisted notification
decisions and the notification/provider tables. Historical notification decisions
remain null. Fresh databases already include all of these changes.

In five additional terminals, activate the environment and export the same
`DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS` and `TEMPORAL_ADDRESS`, then run one
command per terminal:

```bash
python -m app.outbox_publisher
python -m app.consumers.notifications
python -m app.consumers.analytics
python -m app.consumers.reminders
python -m app.temporal_worker
```

Run **one outbox publisher instance** for this milestone. `python -m app.worker`
is also an entry point for the intent-only notification consumer; it no longer
calls the mock provider. The earlier provider exercise remains in
`app/notifications.py` and `app/mock_provider.py`. The shared fake provider is now
also used by Temporal reminder activities; the Kafka notification consumer still
records confirmation/cancellation intents only.

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
    "occurred_at": "2026-10-17T10:00:00Z",
    "scheduled_for": "2099-10-17T10:00:00Z"
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
not mean that Kafka, Temporal or any consumer has completed processing. Kafka being
unavailable does not prevent accepting new events while PostgreSQL is available.
There is no longer a Kafka-related `503` from `POST /events`.

Pydantic rejects unknown fields and unsupported event types. Identifiers contain
1–128 ASCII letters, digits, underscores or hyphens. `event_version` is a positive
32-bit integer; it records the appointment event sequence, but sequence order is
not enforced yet. `occurred_at` and any supplied `scheduled_for` require timezones
and are normalized to UTC. New scheduling events require a future `scheduled_for`;
missing, null, naive, invalid or past appointment times return `422`. Cancellation
can omit the time and never changes the appointment's existing scheduled time.
The request model allows absent times for historical event decoding; the new
scheduling transaction enforces the business requirement before commit.

Each accepted event inserts its request fingerprint, stored successful HTTP
response, notification decision and immutable outbox payload in the same
transaction as the appointment change. The payload is a snapshot of the validated
request, including `scheduled_for` when supplied, plus server-owned
`schedule_generation`. Publication never rebuilds it from current state.

Scheduling inserts a new `SCHEDULED` appointment; an existing appointment conflicts.
`APPOINTMENT_RESCHEDULED` requires a future time and a matching active appointment;
it updates the time and increments `schedule_generation` under a row lock. It
returns `404` for no appointment/patient match and `409` for a cancelled
appointment. Retries of the same event do not increment the generation again.
The generation is server-owned; clients cannot supply it in POST payloads.

Cancellation locks an existing appointment matching both appointment and patient
IDs, then updates status, `last_event_id` and `updated_at`. No match raises
`LookupError` and rolls back the event and outbox. Matching patient IDs enforces
consistency; it is not authorization. Every database failure rolls back all
writes in that transaction.

Distinct repeated cancellations are accepted and each gets an event and outbox
row. Only a transition from `SCHEDULED` to `CANCELLED` warrants a cancellation
intent. Scheduling warrants a confirmation intent. Rescheduling records no new
confirmation intent; its generation gets a reminder intent when delivery is due. These decisions are stored
on the event before commit, so a delayed consumer does not infer historical
transitions from later appointment state.

`event_id` is the request identity. SHA-256 of normalized, sorted JSON
fingerprints all request fields, including the normalized appointment time.
Changing the time under the same event ID returns `409`; an equivalent timezone
representation replays the original outcome. Absent/null optional times are
omitted from canonical JSON, preserving historical six-field fingerprints.
Server-owned generation metadata is excluded from request hashing and checked
separately against the stored event by every consumer.
The API attempts an insert first; PostgreSQL's
primary key resolves concurrent requests. Only `UniqueViolation` from
`events_pkey` triggers replay handling, **after rollback** of the failed
transaction. The same fingerprint returns the stored status/body and `Location`
without another state change or outbox row. Different content returns `409`.
`appointments_pkey` becomes `409`; unrelated database failures remain `500`.
No pre-insert existence check is used. Only successful requests retain outcomes.
Future-time validation runs only after a new event insert succeeds, within that
same transaction. An identical committed retry therefore still returns its
original `201` after the appointment time passes. Event reads and Kafka decoding
do not reject historical timestamps as though they were new scheduling requests.

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
| `app.consumers.reminders` | `careflow-reminders` | Start/verify generation workflows, signal cancellation/invalidation, and persist acknowledged handoff receipts before committing offsets |

Separate groups each receive all topic records. Kafka distributes a group's
partitions among its members. Within each worker, records are processed
synchronously. All consumers validate the payload and appointment key, and
compare its fingerprint with the committed event before writing anything.

The notification primary key `(event_id, notification_type)` and analytics
primary key `event_id` make repeated deliveries harmless. Existing `SENT` rows
from the previous exercise are preserved. The Kafka notification consumer records
intents only. Temporal activities create a related `APPOINTMENT_REMINDER` intent
and update its outcome after calling the local fake provider.

Both `enable.auto.commit` and `enable.auto.offset.store` are disabled. After the
handler durably completes its database work or Temporal handoff, the worker synchronously commits
`record.offset + 1` as the next position to read and checks partition errors in
the commit result. A crash after the database commit and before the offset commit
replays the record; unique constraints prevent another durable outcome.
[Confluent commit API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit)

Database, Kafka commit and transient Temporal handoff errors receive three
attempts, with waits of one and
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

## Reliable reminders — Milestones 4.3 and 4.4

Compose runs a persistent Temporal development server at `localhost:7233`, with
UI at <http://localhost:8233> and namespace `default`. The `temporal_data` volume
stores its SQLite execution history. PostgreSQL remains the source of truth for
appointment state and dispatch authorization. `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE`, and `TEMPORAL_TASK_QUEUE` configure clients/workers; the
queue defaults to `careflow-reminders`. Worker and consumer must agree on it.

### Schedule generations and cancellation

Scheduling creates generation 1. `APPOINTMENT_RESCHEDULED` changes the appointment
time and increments the generation atomically with its event/outbox writes.
Rescheduling a cancelled appointment is rejected; it does not implicitly reopen
it. Concurrent changes serialize on the appointment row. The API returns the
current generation on `GET /appointments/{id}`.

Workflow IDs are `appointment-reminder:{appointment_id}` for generation 1 and
`appointment-reminder:{appointment_id}:g{generation}` for later generations.
Delivery identity and provider keys also include the generation. A stale,
unclaimed generation cannot dispatch after rescheduling, even if its Kafka
signal is delayed, its old workflow wakes up, or events are replayed out of order.

The reminder Kafka consumer uses committed event metadata to identify which
generation to affect. On rescheduling, it signals the previous generation and
starts/verifies the new one. On cancellation, it ensures the matching generation
exists and sends its idempotent `cancel` signal. Creating it first also handles a
cancellation arriving before its scheduling handoff; the final PostgreSQL state
check suppresses delivery if that workflow wakes before the signal is processed.

The workflow's signal handler sets a cancellation flag. Its deterministic
`workflow.wait_condition(..., timeout=remaining_delay)` waits for either that
flag or the reminder deadline. Cancellation while waiting returns `CANCELLED`
without attempting delivery. At the deadline it executes a delivery activity;
all database and provider calls are outside workflow code.
[Temporal signals](https://docs.temporal.io/develop/python/workflows/message-passing)

**Kafka cancellation offsets advance only after the signal is accepted by
Temporal**, not merely queued locally. `reminder_handoffs` records the durable
acknowledgment before the offset commit. A crash after acknowledgment but before
the receipt causes a harmless repeated signal. A crash after the receipt but
before the offset commit can recover from that receipt without another network
call. Invalid payloads still fail identity/generation verification before replay.

Signals cannot be sent to a closed execution. After verifying its type, inputs,
queue and terminal state, the consumer records `ALREADY_CLOSED` and acknowledges
that cancellation event. It does not reopen an execution or retract a delivery.
This is an explicit exception to signaling a running workflow, with a durable
receipt. Provider state and `notification_deliveries` remain available for audit.
Temporal failure holds the Kafka offset, with the existing bounded retry/stop
policy. PostgreSQL cancellation takes effect even while Temporal is unavailable.

### Dispatch cutoff and delivery state

`notification_deliveries` extends an existing `notifications` intent through a
foreign key on `(event_id, notification_type)`. Its unique logical identity is
`(appointment_id, schedule_generation, APPOINTMENT_REMINDER)`. It stores the
immutable provider payload, stable key, claim/attempt timestamps, attempt count,
last error, invalidation time/reason, provider message ID, and outcome.

The activity locks the appointment, validates its committed scheduling event,
and creates the related intent/delivery row. For a new dispatch it checks that
the appointment is still `SCHEDULED`, its generation/time match, and the reminder
is due. It then commits `DISPATCHING` with `claimed_at` before releasing the lock
and contacting the provider. Cancellation and rescheduling take the same lock,
so transaction commit order defines the cutoff.

| Ordering | Outcome |
| --- | --- |
| Cancellation commits before the dispatch claim | No provider request; the activity records `SUPPRESSED` if invoked |
| Rescheduling commits before an old generation claims | Old attempt is `SUPPRESSED`; the new generation has its own timer/key |
| Dispatch claim commits first | That logical request remains authorized and may complete/retry, even after cancellation or rescheduling |
| Provider accepted, response was lost | Retry uses the same key and obtains the same provider message ID |
| Cancellation arrives after successful workflow completion | Record `ALREADY_CLOSED`; delivery remains `SENT` |

**This guarantees no new logical dispatch claim after committed cancellation.**
It does not guarantee zero network requests after cancellation. A worker may
pause or crash after the claim but before provider I/O; its authorized request
may be accepted later. The same claim's retries remain authorized so uncertain
provider acceptance can be resolved safely. Cancellation cannot retract them.
`invalidated_at` and `invalidation_reason` record cancellation/rescheduling after
claim; `invalidated_after_claim` in the outcome exposes that race explicitly.
No database lock or transaction is held during provider I/O.

### Retries, provider idempotency and uncertainty

The fake provider commits accepted keys/payloads/message IDs in a separate
connection. `appointment-reminder:{appointment_id}:g{generation}` is stable
across activity retries, worker restarts and workflow runs. The same key and
payload return the same UUID; different content under that key is rejected.
Concurrent attempts therefore create one provider acceptance. `SENT` means the
fake provider accepted the request, not that a patient received an SMS.

The delivery activity has a 15-second start-to-close timeout, two-minute total
budget, and exponential retry delays starting at one second, doubling to a
30-second cap, with at most five attempts. The worker limits concurrent
activities to its four execution threads. Input/identity errors are nonretryable.
[Temporal retry and timeout handling](https://docs.temporal.io/develop/python/best-practices/error-handling)

If the retry budget expires, a separate retryable finalization activity records
`UNCERTAIN` and marks its notification intent `UNKNOWN`. It never downgrades a
known `SENT` result and does not assume that a timed-out request failed. A late
response from an already-authorized attempt may resolve the database row to
`SENT` after the workflow returned `UNCERTAIN`; the workflow result describes
what was known at completion. Uncertain deliveries require investigation rather
than automatically creating another logical request. If finalization itself
cannot reach PostgreSQL, the workflow fails and its durable `DISPATCHING` row
still requires reconciliation; it is not silently acknowledged as success.

For controlled local failures, set these only on the Temporal worker:

```bash
MOCK_PROVIDER_TIMEOUT_AFTER_ACCEPT_ONCE=1 python -m app.temporal_worker
# Or make every provider request fail before acceptance:
MOCK_PROVIDER_FAIL_BEFORE_ACCEPT=1 python -m app.temporal_worker
```

The first flag loses the first response for each newly accepted key. Acceptance
is persisted, so a restarted worker's retry returns the original message ID.
The second exercises retry exhaustion. Neither flag calls a real SMS service.

### Upgrades and verification

Historical payloads and request fingerprints are not rewritten. Existing
Milestone 4.1/4.2 demo workflow inputs retain their original, side-effect-free
command paths for replay compatibility. They do not suddenly send notifications
on upgrade. Explicitly rescheduling an existing appointment creates a new
generation using the delivery-enabled protocol. The standalone `start_reminder`
CLI remains a timing-only demo; only committed scheduling events supply the
source event identity needed to authorize delivery.

Workflow-ID reuse remains rejected within Temporal retention. Durable delivery
identity/provider keys and handoff receipts add database deduplication beyond
that retention window. Rescheduling intentionally creates a new logical reminder;
an earlier already-claimed reminder can still complete under the cutoff policy.

Inspect the state with:

```bash
docker compose exec db psql -U careflow -d careflow -c 'TABLE notification_deliveries;'
docker compose exec db psql -U careflow -d careflow -c 'TABLE reminder_handoffs;'
docker compose exec temporal temporal workflow show --workflow-id appointment-reminder:appt_123
```

Tests cover cancellation before dispatch, an in-flight activity accepting its
signal while the authorized provider request completes, rescheduling and stale
attempts, duplicate/concurrent attempts, durable signals before offset commits,
consumer restart after signal acceptance, already-closed workflows, acceptance
followed by timeout, five-attempt exhaustion, and worker SIGKILL/restart during
a generation timer. Migrations preserve existing notification outcomes.

```bash
pytest -q tests/test_delivery.py tests/test_temporal.py
```

Set all three `TEST_*` server variables shown below for the full suite.

## Tables

See [app/schema.sql](app/schema.sql),
[app/notification_schema.sql](app/notification_schema.sql) and
[app/messaging_schema.sql](app/messaging_schema.sql) and
[app/reminder_schema.sql](app/reminder_schema.sql).

| Table | Purpose |
| --- | --- |
| `events` | Accepted immutable request fields including nullable historical `scheduled_for`, successful responses and notification decisions; primary key `event_id` |
| `appointments` | Current state and nullable historical `scheduled_for`; primary key `appointment_id` |
| `outbox` | Immutable publication snapshots and `published_at`; primary key `event_id`, partial pending index on `(created_at, event_id)` |
| `notifications` | Unique event/type intents, including reminder intents linked to delivery outcomes |
| `notification_deliveries` | Dispatch claim, attempts, stable provider identity and outcome per appointment/generation |
| `reminder_handoffs` | Durable start/signal acknowledgment or already-closed decision per event |
| `analytics_events` | Deduplicated activity with appointment, event type, source time and processing time |
| `mock_provider_acceptances` | Independent durable acceptance/deduplication storage for the local fake provider |

`idx_events_appointment_id` indexes appointment event-history lookups. Primary
keys also create indexes. There is no patient demographics table.

## Verify

With PostgreSQL, Kafka and Temporal running and `appointments.events` initialized:

```bash
export TEST_DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export TEST_KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export TEST_TEMPORAL_ADDRESS='localhost:7233'
pytest -q
```

Each database test creates and removes its own temporary schema. Broker tests
create isolated three-partition topics and consumer groups; application records
are untouched. The test database role needs schema creation permission. Database
tests require real PostgreSQL. Broker tests explicitly skip if their environment
variable is unset; set all three variables above for the full suite.

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
connection pooling, burst control, real SMS integration, automated reconciliation
of uncertain deliveries, Redis and production observability remain outside these milestones.
