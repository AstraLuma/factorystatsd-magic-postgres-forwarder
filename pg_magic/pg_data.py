"""
Dealing with postgres in a data capacity.
"""
from typing import Iterable

from .pg_conn import fetch


def _flatten_signals(data: dict) -> dict:
    """
    Goes from

        [{"signal":{"type":"item","name":"uranium-rounds-magazine"},"count":102400}]}, ...]

    to

        {"uranium-rounds-magazine": 102400}
    """

    return {
        signal['signal']['name']: signal['count']
        for signal in data
    }


def add_samples(conn, time: int, entities: list[dict]):
    """
    Adds the given samples to the data set.
    """
    # FIXME: actual schema
    with conn.cursor() as cur, cur.copy("COPY __raw__ (time, name, tags, surface_index, circuit, data) FROM STDIN") as copy:
        for ent in entities:
            tags = {
                left: right if eq else None 
                for left, eq, right in map(str.partition, ent['settings']['tags'].split(','))
            }
            if 'red_signals' in ent:
                copy.write_row((
                    time,
                    ent['settings']['name'],
                    tags,
                    ent.get('surface_index', None),
                    "red",
                    _flatten_signals(ent['red_signals'])
                ))

            if 'green_signals' in ent:
                copy.write_row((
                    time,
                    ent['settings']['name'],
                    tags,
                    ent.get('surface_index', None),
                    "green",
                    _flatten_signals(ent['green_signals'])
                ))


def read_names(conn) -> Iterable[str]:
    """
    Reads all existing stat names.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT name FROM __raw__")
        for row in fetch(cur):
            yield row.name
