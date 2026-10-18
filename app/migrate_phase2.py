from pathlib import Path

from app.db import open_connection


def migrate(connection) -> None:
    with connection.transaction():
        connection.execute("""
            ALTER TABLE events ADD COLUMN notification_type TEXT CHECK (
                notification_type IN ('APPOINTMENT_SCHEDULED', 'APPOINTMENT_CANCELLED')
            )
        """)
        connection.execute(Path(__file__).with_name("notification_schema.sql").read_text())


def main() -> None:
    with open_connection() as connection:
        migrate(connection)
    print("Added notification decisions, notification outcomes and durable mock provider storage.")


if __name__ == "__main__":
    main()
