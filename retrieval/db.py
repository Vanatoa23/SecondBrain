"""Database connection helper for the Second Brain retrieval engine.

Read-only usage — retrieval never modifies data.
All queries use parameterized statements exclusively.
"""

import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Example: postgresql://postgres:postgres@localhost:5432/secondbrain"
        )
    return url


def _register_vector_loader(conn) -> None:
    """Load pgvector ``vector`` columns as numpy arrays via pgvector.psycopg.

    ``register_vector(conn)`` registers the official loaders (text + binary,
    correct endianness); with numpy installed they return ``np.ndarray``,
    which vector search / dedup / ingest index directly. Silently does
    nothing when the pgvector extension (or package) is absent.
    """
    try:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    except Exception:
        pass  # pgvector extension not installed — embedding column unused


@contextmanager
def get_connection():
    """Yield a read connection with dict-row factory and numpy vector loader."""
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    _register_vector_loader(conn)
    try:
        yield conn
    finally:
        conn.close()
