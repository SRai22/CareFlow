# CareFlow

Phase 1 of an event-driven patient workflow learning project: a synchronous
FastAPI API backed by PostgreSQL. Python 3.12+ and Docker Compose are required for
the setup below. Only the database runs in Docker.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
docker compose up -d --wait db
export DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
python -m app.init_db
uvicorn app.main:app --reload
```

Run `app.init_db` **once on an empty database**. Schema creation is transactional
and fails if the tables already exist; it does not migrate existing tables.
The named Docker volume retains data across container restarts. `DATABASE_URL`
is required by the API and initialization command. The credentials in Compose
are for local development; use synthetic patient identifiers.

For a database created before request fingerprints and stored outcomes were
added, stop the API and run this migration **once**, then restart the API:

```bash
python -m app.migrate_idempotency
```

Use the same `DATABASE_URL` as above. The migration adds the three columns and
backfills existing committed events using the previous fixed `201` event
response contract. All changes commit together; new databases already include
these columns through `app.init_db`.

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
| `POST /events` | `201`, stored event plus `received_at`; matching retries replay the stored status/body and `Location` | `409` event ID reused with different content or existing appointment on scheduling; `404` cancellation has no matching appointment/patient; `422` invalid payload; `500` unexpected database failure |
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
| `events` | Accepted event history and successful request outcomes, with no update/delete endpoint | `event_id` primary key; event fields, `received_at`, `request_hash`, `response_status`, `response_body` |
| `appointments` | Current state for quick reads | `appointment_id` primary key; patient ID, status, `last_event_id` foreign key, `updated_at` |

No patient table is needed: patients are external identifiers and we do not yet
store patient demographics. No notification table is needed until notification
work is introduced. Primary keys index the two GET lookups.
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

With PostgreSQL running:

```bash
export TEST_DATABASE_URL='postgresql://careflow:careflow@localhost:5432/careflow'
pytest -q
```

Tests use a unique temporary PostgreSQL schema per test and remove only that
schema afterward. Existing application tables and data are untouched. The test
role needs permission to create schemas. Tests intentionally require PostgreSQL;
there is no SQLite substitute or silent skip when a database is unavailable.

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

```bash
pytest -v tests/test_transactions.py
```

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
- Connections are opened per request. Pooling, burst handling, authentication,
  notifications, Kafka, Temporal, Redis and observability are later work.

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
reapplying the operation. Processing remains synchronous; ordering, delivery
recovery and broader concurrency guarantees are follow-up milestones.”
