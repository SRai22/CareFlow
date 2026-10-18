import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from app.main import app
from app.kafka import get_publisher


class RecordingPublisher:
    def __init__(self):
        self.events = []

    def publish(self, event):
        self.events.append(event)


@pytest.fixture
def publisher():
    return RecordingPublisher()


@pytest.fixture
def database_url(monkeypatch):
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("Set TEST_DATABASE_URL to a PostgreSQL database (see README).")
    schema = f"careflow_test_{uuid4().hex}"
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        test_url = make_conninfo(url, options=f"-csearch_path={schema}")
        try:
            with psycopg.connect(test_url) as connection:
                for filename in ("schema.sql", "notification_schema.sql"):
                    connection.execute(
                        (Path(__file__).parents[1] / "app" / filename).read_text()
                    )
            monkeypatch.setenv("DATABASE_URL", test_url)
            yield test_url
        finally:
            # Only this test's randomly named schema is removed.
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


@pytest.fixture
def connection(database_url):
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as conn:
        yield conn


@pytest.fixture
def client(database_url, publisher):
    app.dependency_overrides[get_publisher] = lambda: publisher
    try:
        with TestClient(app, raise_server_exceptions=False) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.pop(get_publisher, None)


@pytest.fixture
def event():
    return {
        "event_id": "evt_001",
        "patient_id": "patient_123",
        "appointment_id": "appt_456",
        "event_type": "APPOINTMENT_SCHEDULED",
        "event_version": 1,
        "occurred_at": "2026-10-17T10:00:00Z",
    }
