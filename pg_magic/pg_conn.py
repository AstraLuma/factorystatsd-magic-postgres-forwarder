import contextlib
import logging
from typing import Iterable

import psycopg
from psycopg.rows import namedtuple_row
from psycopg.types import TypeInfo
from psycopg.types.hstore import register_hstore

LOG = logging.getLogger(__name__)


def fetch(cur) -> Iterable:
    """
    Performs batched fetching.
    """
    rows = cur.fetchmany()
    while rows:
        yield from rows
        rows = cur.fetchmany()


@contextlib.contextmanager
def connection(connstr, **opts):
    """
    Connects to postgresql, enabling things like hstore (if available).

    Sets up namedtuple rows, so results can be used by name or index.

    Sets autocommit.
    """
    LOG.info(f"Connecting to {connstr}")
    with psycopg.connect(connstr, autocommit=True, row_factory=namedtuple_row, **opts) as conn:
        # Register hstore, if available
        info = TypeInfo.fetch(conn, "hstore")
        if info:
            register_hstore(info, conn)

        yield conn
