from pathlib import Path

from app.db import open_connection


def migrate(connection) -> None:
    with connection.transaction():
        connection.execute(Path(__file__).with_name("reminder_schema.sql").read_text())


def main() -> None:
    with open_connection() as connection:
        migrate(connection)
    print("Added schedule generations, reminder delivery outcomes and handoff receipts.")


if __name__ == "__main__":
    main()
