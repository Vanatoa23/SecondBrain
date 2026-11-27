"""Database connection helper for the Second Brain ingestion pipeline.

All queries use parameterized statements exclusively — never interpolate
user content into SQL strings.
"""

import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def get_database_url() -> str:
    """Return the Postgres connection string from the environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Example: postgresql://postgres:postgres@localhost:5432/secondbrain"
        )
    return url


def _register_vector_loader(conn) -> None:
    """Register pgvector's official ``vector`` type loaders/dumpers.

    ``register_vector(conn)`` registers the text + binary loaders (correct
    endianness) and dumpers for ``Vector`` / numpy arrays, so query params
    that are ndarrays adapt cleanly. Silently does nothing when the pgvector
    package or extension is not installed.
    """
    try:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    except Exception:
        pass  # pgvector extension not installed — embedding column unused


def as_numpy(value):
    """Convert a pgvector column value to a float32 ndarray.

    ``register_vector`` loaders return ``pgvector.Vector`` objects (which
    numpy cannot convert directly); older/custom loaders return ndarrays or
    plain lists. Consumers (dedup.find_duplicate, ingest.auto_link) use this
    instead of ``np.array(row["embedding"], dtype=np.float32)``.
    """
    import numpy as np

    if value is None:
        return None
    if isinstance(value, np.ndarray):
        return value.astype(np.float32, copy=False)
    if hasattr(value, "to_numpy"):  # pgvector.Vector
        return value.to_numpy().astype(np.float32, copy=False)
    return np.asarray(value, dtype=np.float32)


@contextmanager
def get_connection():
    """Yield a psycopg connection with dict-row factory and pgvector
    loaders registered. Auto-commits on success."""
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    _register_vector_loader(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
