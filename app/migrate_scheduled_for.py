from app.db import open_connection


def migrate(connection) -> None:
    # Preserve historical requests, hashes, outcomes and immutable outbox payloads.
    with connection.transaction():
        connection.execute("ALTER TABLE appointments ADD COLUMN scheduled_for TIMESTAMPTZ")
        connection.execute("ALTER TABLE events ADD COLUMN scheduled_for TIMESTAMPTZ")


def main() -> None:
    with open_connection() as connection:
        migrate(connection)
    print("Added nullable scheduled_for columns; historical appointment times remain unknown.")


if __name__ == "__main__":
    main()
