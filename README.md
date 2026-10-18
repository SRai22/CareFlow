# CareFlow

CareFlow Phase 2A: FastAPI persists appointment events in PostgreSQL, publishes
new events to Kafka, and a separate notification worker uses a durable mock
provider. Python 3.12+ and Docker Compose are required. PostgreSQL and Kafka run
in Docker; the API and worker run locally.

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

Run `app.init_db` **once on an empty database**. Schema creation is transactional
and fails if the tables already exist; it does not migrate existing tables.
The named Docker volume retains data across container restarts. `DATABASE_URL`
is required by the API and initialization command. The credentials in Compose
are for local development; use synthetic patient identifiers. Kafka also has a
named volume, retaining records and committed consumer offsets across restarts.
Its one-shot storage initializer assigns the volume to UID 1000 before the
unprivileged broker starts; otherwise a fresh root-owned volume prevents startup.

For an existing Phase 1 database, run the following **once** instead of
`app.init_db`, before starting the upgraded API or worker:

```bash
python -m app.migrate_phase2
```

This adds the notification decision column and both notification/provider
tables in one transaction. Historical events retain a null notification decision;
the upgrade does not generate or publish historical notifications.

For a database created before request fingerprints and stored outcomes were
added, stop the API and run this migration **once**, then restart the API:

```bash
python -m app.migrate_idempotency
```

Use the same `DATABASE_URL` as above. The migration adds the three columns and
backfills existing committed events using the previous fixed `201` event
response contract. All changes commit together; new databases already include
these columns through `app.init_db`.

If upgrading an older database, apply `migrate_idempotency` before `migrate_phase2`.

In a second terminal, activate the environment and export the same variables:

```bash
source .venv/bin/activate
export DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
python -m app.worker
```

The worker subscribes to `appointments.events` in the `notifications` consumer
group. The topic initializer creates exactly three partitions with replication
factor one. This is a single-broker local setup. Host clients use `localhost:9092`;
clients on the Compose network can use `kafka:29092`.

Interactive API docs: <http://127.0.0.1:8000/docs>.

```bash
curl -i http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "evt_001",
    "patient_id": "patient_123",
    "appointment_id": "appt_456",
    "event_type": "APPOINTMENT_SCHEDULED",
    "event_version": 1,
    "occurred_at": "2026-10-17T10:00:00Z"
  }'

curl http://127.0.0.1:8000/events/evt_001
curl http://127.0.0.1:8000/appointments/appt_456
```

The appointment response has this shape (the timestamp records processing time):

```json
{
  "appointment_id": "appt_456",
  "patient_id": "patient_123",
  "status": "SCHEDULED",
  "last_event_id": "evt_001",
  "updated_at": "2026-10-17T10:00:01Z"
}
```

## API contract

| Endpoint | Success | Errors |
| --- | --- | --- |
| `POST /events` | `201`, stored event plus `received_at`; new events publish to Kafka; matching retries replay the stored outcome without publishing | `409` conflicting ID or existing appointment; `404` cancellation has no matching appointment/patient; `422` invalid payload; `500` unexpected database failure; `503` publication failed after database commit |
| `GET /events/{id}` | `200`, stored event plus `received_at` | `404` unknown ID; `422` invalid ID |
| `GET /appointments/{id}` | `200`, current appointment state | `404` unknown ID; `422` invalid ID |

IDs contain 1–128 ASCII letters, digits, underscores or hyphens. `event_version`
is a positive 32-bit integer. For this project it denotes the appointment's event
sequence number, **not a payload schema version**. Phase 1 records it without
enforcing sequence order. `occurred_at` must include a timezone. PostgreSQL
preserves the instant, not the original timezone offset. Unknown payload fields
and unsupported event types are rejected.

## Tables and state changes

The complete schema is in [app/schema.sql](app/schema.sql).

| Table | Purpose | Key fields |
| --- | --- | --- |
| `events` | Accepted event history, successful request outcomes and notification decisions | `event_id` primary key; event fields, `received_at`, `request_hash`, `response_status`, `response_body`, nullable `notification_type` |
| `appointments` | Current state for quick reads | `appointment_id` primary key; patient ID, status, `last_event_id` foreign key, `updated_at` |
| `notifications` | Durable notification processing outcomes | `(event_id, notification_type)` primary key; unique `provider_key`, `PENDING`/`SENT`, provider message ID, timestamps |
| `mock_provider_acceptances` | Independent durable mock provider deduplication | `idempotency_key` primary key; request hash/payload, stable message UUID, acceptance time |

No patient table is needed: patients are external identifiers and we do not yet
store patient demographics. Primary keys index the two GET lookups.
`idx_events_appointment_id` indexes `events(appointment_id)` for future queries
that retrieve an appointment's event history.

For databases initialized before this index was added, run the following SQL
once; do not rerun `app.init_db` on an existing database:

```sql
CREATE INDEX idx_events_appointment_id
ON events(appointment_id);
```

For each valid POST, one PostgreSQL transaction:

1. Inserts the event, normalized request hash and successful HTTP outcome. These
   remain uncommitted until the appointment write succeeds. The primary key
   rejects a reused event ID.
2. Routes `APPOINTMENT_SCHEDULED` to `schedule_appointment`, which inserts a new
   appointment. Its primary key rejects an existing appointment without an
   existence query.
3. Routes `APPOINTMENT_CANCELLED` to `cancel_appointment`, which updates only an
   existing appointment matching both appointment and patient IDs. Matching zero
   rows raises `LookupError` and rolls back the event.
4. Commits both writes before returning `201`. Any failure rolls back both.

After commit, the route publishes only new events to Kafka, keyed by
`appointment_id`. The payload is the six normalized request fields. HTTP replays
and rejected transactions do not publish. Producer idempotence protects its own
retries; it does not replace the consumer's durable deduplication.

| Incoming event | State if appointment is missing | State if appointment exists |
| --- | --- | --- |
| `APPOINTMENT_SCHEDULED` | Create `SCHEDULED` | Reject with `409`; roll back the event |
| `APPOINTMENT_CANCELLED` | Reject with `404`; roll back the event | Set `CANCELLED` if patient ID matches; otherwise `404` |

Cancellation requires an existing appointment belonging to the supplied patient.
The update changes `status`, `last_event_id` and `updated_at` atomically with the
event insert. Matching patient IDs enforces data consistency; authorization is
a separate requirement. Repeated cancellations with distinct event IDs are
accepted and recorded, including when the appointment is already `CANCELLED`.
A schedule event targeting an existing appointment is rejected regardless of
the appointment's status. Version comparisons remain deferred.

## Phase 2A notification processing

The transaction records its notification decision before commit:

| Accepted operation | Notification decision |
| --- | --- |
| New scheduling | Confirmation (`APPOINTMENT_SCHEDULED`) |
| Cancellation changing `SCHEDULED` to `CANCELLED` | Cancellation notice (`APPOINTMENT_CANCELLED`) |
| Another cancellation when already cancelled | No new notice; the event is still published |

Cancellation locks the appointment row while deciding whether it changes state.
The decision is persisted on the event, so the worker does not infer historical
transitions from an appointment's later state. Each Kafka payload must match its
committed event fingerprint and use the correct appointment key.

For a warranted notification, the worker first commits a unique `PENDING` row.
It then locks that row, skips an already `SENT` notification, or calls the mock
provider using the stable key `<event_id>:<notification_type>`. The provider
records acceptance in a **separate autocommit connection**, so its accepted key
and message UUID survive a worker rollback or restart. Retrying the same key
and payload returns that UUID. Reusing a provider key with different content
fails. There are no external SMS calls.

After provider acceptance, the worker records `SENT` and the message UUID and
commits PostgreSQL. Here `SENT` means the mock accepted the request. Kafka offset
`record.offset + 1` is committed synchronously only afterward. Both automatic
offset commits and automatic offset storage are disabled. An event that warrants
no notification is acknowledged after the persisted decision is checked.

Database, provider and offset-commit errors get three attempts, with waits of
one and two seconds. A failed offset commit retries processing safely: `SENT`
prevents another send. After exhaustion the worker exits nonzero without
advancing the failed record's offset. Invalid JSON, incorrect keys, fingerprint
mismatches and unexpected errors fail immediately. Closing the consumer does
not commit offsets. Repair the cause and restart the worker; it re-reads the
uncommitted record. There is no dead-letter queue or automatic supervisor yet.

The consumer uses Confluent's documented
[manual offset commits](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit).

## Deliberate database-to-Kafka failure window

Phase 2A publishes directly **after** the PostgreSQL commit. No outbox is used.
If the process dies between commit and publish, the event and appointment exist
but no Kafka record is guaranteed. If publication fails or acknowledgement times
out, the API returns `503` with a detail explicitly stating that the event was
already committed. The stored `201` outcome describes PostgreSQL success; a
matching HTTP retry replays it and **does not attempt publication again**.
The actual broker outcome can also be uncertain after a timeout.

This is intentional: HTTP idempotency does not repair the publication gap.
An outbox is the next milestone. Broker retries or later manual republication
can also create duplicate records, which the worker tolerates.

The producer waits for delivery acknowledgement via `flush()` and its delivery
callback, rather than treating a local enqueue as a successful publish.
[Confluent producer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Producer.produce)

The request uses the Pydantic `EventCreate` model in
[app/schemas.py](app/schemas.py). FastAPI validates the body before executing the
route. `UniqueViolation` on `events_pkey` enters the retry comparison described
below; `appointments_pkey` becomes `409`. Other database errors propagate as
`500` without exposing SQL details.

## Idempotent retries

`event_id` identifies a logical request. `request_hash` is SHA-256 of the
validated payload serialized with sorted JSON keys and no formatting whitespace.
All six request fields participate. `occurred_at` is normalized to UTC, so
equivalent timezone offsets and JSON field order produce the same fingerprint.

The event row also stores `response_status` and `response_body` (JSONB). They
commit atomically with the appointment change. No `PROCESSING` state is needed
under this transaction contract. Timestamps describe source and recording times;
the stored outcome determines the response to a retry.

The API first attempts the insert, retaining the primary key as the concurrency
guard. If `events_pkey` raises `UniqueViolation`, the transaction rolls back
before the API reads the existing row:

- Same hash: return the stored successful status and body, with the original
  `Location`, without changing the appointment or recording another event.
- Different hash: return `409`; neither the original event nor appointment is
  changed.

PostgreSQL's uniqueness check makes a competing insert wait for the first
transaction's outcome. If it commits, the retry reads the stored outcome; if it
rolls back, the waiting insert can proceed. There is no pre-insert existence
query. Retries of an old scheduling event still replay its original result even
if a later cancellation has changed the appointment's current state.

Only successful requests are retained. Validation errors, lookup failures,
appointment conflicts and database failures leave no successful outcome for the
new event ID. The internal fingerprint and response columns are not exposed by
`GET /events/{id}`.

Explicit parameterized SQL keeps the schema, queries and transaction boundary
visible while learning SQL. Synchronous routes match the synchronous Psycopg
driver; each request gets its own connection. The transaction uses
[Psycopg's transaction context](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).
The lookup indexes come from PostgreSQL's
[primary key constraints](https://www.postgresql.org/docs/17/ddl-constraints.html).

## Verify

With PostgreSQL and Kafka running and the topic initialized:

```bash
export TEST_DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
export TEST_KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
pytest -q
```

Tests use a unique temporary PostgreSQL schema per test and remove only that
schema afterward. Existing application tables and data are untouched. The test
role needs permission to create schemas. Tests intentionally require PostgreSQL;
there is no SQLite substitute or silent skip when a database is unavailable.
Broker tests create and delete their own topics and use isolated consumer groups.
They also verify that the configured `appointments.events` topic has three
partitions. If `TEST_KAFKA_BOOTSTRAP_SERVERS` is unset, broker-dependent tests are
explicitly skipped; set it as above for the complete integration suite.

Coverage includes all three endpoints, input validation, scheduling/cancellation,
event history, conflicting payload rejection, patient mismatch and missing-appointment
rollback, timezone handling, and a database trigger that deliberately fails
appointment inserts and cancellation updates to verify atomic rollback. Tests
also cover scheduling conflicts for both scheduled and cancelled appointments,
repeated cancellations, and an unrelated unique constraint returning `500`.

The five Phase 1 transaction checks are in
[tests/test_transactions.py](tests/test_transactions.py). They call
[the transaction functions](app/transactions.py) directly and inspect committed data through
a separate PostgreSQL connection. Missing appointments and patient mismatches
raise `LookupError`; duplicate IDs raise `UniqueViolation` for `events_pkey`.
A test-only `CHECK` constraint rejects the cancellation update to prove that its
event insert also rolls back. The API maps cancellation lookup failures to `404`
and appointment conflicts to `409` after rollback. A raw transaction call still
raises `UniqueViolation` for duplicate event IDs; the API handles outcome replay.

[tests/test_idempotency.py](tests/test_idempotency.py) covers matching retries of
scheduling and cancellation, all five possible changed fields under the same
event ID, timezone and JSON order normalization, retries after later state
changes, concurrent identical and conflicting requests, retry after failure,
and migration of historical events.

The concurrency test holds Worker A's row uncommitted and observes Worker B
waiting through `pg_blocking_pids` before committing A. B then raises a duplicate
violation, exits the transaction context (which rolls back), and only afterward
reads A's committed outcome. Querying before that rollback would fail and make
the replay test fail.

```bash
pytest -v tests/test_transactions.py
```

The first Phase 2A acceptance check is
`tests/test_kafka.py::test_post_commits_then_publishes_keyed_event_once`: it
persists `appt_123`, reads its Kafka payload and key, then verifies an identical
HTTP retry produces no second record. The automated test uses an isolated topic
with the same three-partition setup to avoid polluting the development worker.
To inspect the real topic while making requests against the running API:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic appointments.events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic appointments.events \
  --from-beginning --property print.key=true
```

[tests/test_notifications.py](tests/test_notifications.py) covers duplicate Kafka
records, independent appointments, committed offsets surviving a real consumer
close/recreation, skipped repeated-cancellation notices, provider failures,
poison records, and a crash after durable provider acceptance but before the
worker records success. The last case restarts the consumer with the same group
and proves the provider acceptance count stays one.

## Phase 2A checklist

- [x] Local Kafka and a three-partition `appointments.events` topic.
- [x] Publish new events after commit, keyed by appointment ID; skip HTTP replays.
- [x] Separate `notifications` consumer worker.
- [x] Durable `PENDING`/`SENT` outcomes with a unique event/type key.
- [x] Durable mock provider idempotency keys and acceptance results.
- [x] Manual offsets, bounded retries and an explicit stop-on-error policy.
- [x] PostgreSQL/Kafka integration tests and failure injection.

## Deliberate boundaries and next exercises

- Successful retries replay their persisted outcomes; reusing an ID for a
  different normalized payload returns `409`. Outcomes currently have no expiry.
- Cancellation before scheduling returns `404` and is rolled back; there is no
  buffering or replay. Concurrent cancellation requests are not ordered by
  version. Simultaneous attempts to schedule the same appointment are resolved
  by its primary key: different event IDs conflict, while identical requests
  under the same event ID replay the successful result.
- `occurred_at` and `event_version` are stored without sequence enforcement.
- If a response is lost after commit, retrying the same ID and payload recovers
  the original successful response without applying the operation again.
- Direct publication can fail after database commit; an HTTP replay does not
  heal it. The outbox pattern remains deliberately deferred.
- Connections are opened per request. Pooling, burst handling, authentication,
  Temporal, Redis and observability are later work.

For the first review, trace both transactions in
[app/transactions.py](app/transactions.py), then cancel an unknown appointment.
Explain why neither the event nor an appointment persists. Next, schedule an
appointment and try scheduling it again with a different event ID; explain why
the appointment primary key causes a conflict and rolls back the new event.

Interview explanation: “I separated accepted event history from current state so
reads do not replay history. Both writes share a PostgreSQL transaction, so a
failure cannot leave an event recorded without its state change. Constraints
guard IDs and allowed values. A normalized request fingerprint distinguishes
retries from conflicting reuse of an event ID, and the successful response is
persisted in that same transaction. The API replays that outcome without
reapplying the operation or publishing another record. New events publish to
Kafka after commit. A separate worker stores notification outcomes and calls a
durable idempotent mock provider before committing Kafka offsets. Direct
publication intentionally leaves a crash window for the outbox milestone.”
