import os
from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg.rows import dict_row


def open_connection() -> psycopg.Connection[dict[str, Any]]:
    # Explicit transaction blocks control writes; reads need no open transaction.
    return psycopg.connect(
        os.environ["DATABASE_URL"],
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=5,
    )


def get_connection() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    with open_connection() as connection:
        yield connection
