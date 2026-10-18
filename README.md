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
| `POST /events` | `201`, stored event plus `received_at`; `Location` header | `409` duplicate event ID, patient mismatch or cancellation of a missing appointment; `422` invalid payload |
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
| `events` | Accepted event history, with no update/delete endpoint | `event_id` primary key; patient/appointment IDs, type, version, `occurred_at`, `received_at` |
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

1. Inserts the event. The primary key rejects a reused event ID.
2. Reads the appointment and rejects a different patient ID.
3. Creates or updates a scheduled appointment, or cancels an existing appointment.
   Updates match both appointment and patient IDs. A cancellation matching zero
   rows fails and rolls back its event.
4. Commits both writes before returning `201`. Any failure rolls back both.

| Incoming event | State if appointment is missing | State if appointment exists |
| --- | --- | --- |
| `APPOINTMENT_SCHEDULED` | Create `SCHEDULED` | Set `SCHEDULED` |
| `APPOINTMENT_CANCELLED` | Reject with `409`; roll back the event | Set `CANCELLED` if patient ID matches |

Cancellation requires an existing appointment belonging to the supplied patient.
The update changes `status`, `last_event_id` and `updated_at` atomically with the
event insert. Matching patient IDs enforces data consistency; authorization is
a separate requirement. In Phase 1, a delayed schedule event can still overwrite
a cancellation of an existing appointment. Version comparisons remain deferred.

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
event history, duplicate rejection, patient mismatch and missing-appointment
rollback, timezone handling, and a database trigger that deliberately fails
appointment inserts and cancellation updates to verify atomic rollback. A test
also records the current arrival-order limitation.

The five Phase 1 transaction checks are in
[tests/test_transactions.py](tests/test_transactions.py). They call
[process_event](app/transactions.py) directly and inspect committed data through
a separate PostgreSQL connection. Missing appointments and patient mismatches
raise `LookupError`; duplicate IDs raise `UniqueViolation` for `events_pkey`.
A test-only `CHECK` constraint rejects the cancellation update to prove that its
event insert also rolls back. The API maps lookup failures and duplicate event
IDs to `409` after rollback.

```bash
pytest -v tests/test_transactions.py
```

## Deliberate boundaries and next exercises

- Duplicate IDs return `409`, including retries with identical payloads. This is
  uniqueness enforcement, not a retry-safe idempotency contract.
- State follows sequential request arrival order. Concurrent requests are not
  ordered: updates can overwrite each other, and simultaneous creation of the
  same appointment can produce a database uniqueness error and `500`.
- `occurred_at` and `event_version` are stored but do not prevent stale updates.
- A commit followed by a lost HTTP response leaves the client uncertain whether
  its request succeeded. It can inspect `GET /events/{id}`; a retry gets `409`.
- Connections are opened per request. Pooling, burst handling, authentication,
  notifications, Kafka, Temporal, Redis and observability are later work.

For the first review, trace the transaction in [app/transactions.py](app/transactions.py), then
schedule an appointment with version 1, cancel it with version 3, then send a
delayed scheduling event with version 2. Explain why the current state is wrong
and what information is already stored to fix it in Phase 3.

Interview explanation: “I separated accepted event history from current state so
reads do not replay history. Both writes share a PostgreSQL transaction, so a
failure cannot leave an event recorded without its state change. Constraints
guard IDs and allowed values. The first version deliberately applies events
synchronously; ordering, safe retries and concurrency control are explicit
follow-up milestones.”
