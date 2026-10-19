from pathlib import Path

from app.db import open_connection


def migrate(connection) -> None:
    # Historical events may already have been published by the previous direct producer.
    with connection.transaction():
        connection.execute(Path(__file__).with_name("messaging_schema.sql").read_text())


def main() -> None:
    with open_connection() as connection:
        migrate(connection)
    print("Created outbox and analytics tables; historical events were not enqueued.")


if __name__ == "__main__":
    main()
