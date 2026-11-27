"""Database connection helper for the Second Brain retrieval engine.

Read-only usage — retrieval never modifies data.
All queries use parameterized statements exclusively.
"""

import json
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
    """Load pgvector ``vector`` columns as numpy float32 arrays.

    Retrieval consumers index ``row["embedding"]`` as a numpy array, so we
    register loaders that decode the vector straight into an ``np.ndarray``
    (pgvector's default loader returns a ``Vector`` object numpy cannot
    convert). Silently does nothing when the pgvector extension is absent.
    """
    try:
        import numpy as np
        from psycopg.abc import Buffer
        from psycopg.adapt import Loader
        from psycopg.pq import Format
        from psycopg.types import TypeInfo
    except ImportError:
        return

    class _VectorTextLoader(Loader):
        format = Format.TEXT

        def load(self, data: Buffer):
            if data is None:
                return None
            if isinstance(data, memoryview):
                data = data.tobytes()
            return np.array(json.loads(data.decode("utf-8")), dtype=np.float32)

    class _VectorBinaryLoader(Loader):
        format = Format.BINARY

        def load(self, data: Buffer):
            if data is None:
                return None
            if isinstance(data, memoryview):
                data = data.tobytes()
            dim = int.from_bytes(data[:4], "little")
            return np.frombuffer(data[4:4 + 4 * dim], dtype="<f4").astype(np.float32)

    try:
        info = TypeInfo.fetch(conn, "vector")
        if info is not None:
            info.register(conn)
            conn.adapters.register_loader(info.oid, _VectorTextLoader)
            conn.adapters.register_loader(info.oid, _VectorBinaryLoader)
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
