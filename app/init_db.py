from pathlib import Path

from app.db import open_connection


def main() -> None:
    # Run once on an empty database; fail instead of hiding schema drift.
    with open_connection() as connection:
        with connection.transaction():
            for filename in ("schema.sql", "notification_schema.sql"):
                connection.execute(Path(__file__).with_name(filename).read_text())
    print("Created CareFlow tables.")


if __name__ == "__main__":
    main()
