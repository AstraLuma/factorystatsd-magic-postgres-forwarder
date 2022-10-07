"""
Dealing with postgres in a schema capacity
"""
import datetime

from psycopg.sql import SQL, Identifier, Literal
from psycopg.types import TypeInfo

from .pg_conn import fetch


def check_extensions(conn):
    """
    Install extensions and other fundamentals.

    Might make the connection object unusable.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS hstore CASCADE")
    # TODO: Forcibly close connection?


def _has_func(conn, name):
    with conn.cursor() as cur:
        cur.execute("""
SELECT
    routine_name
FROM 
    information_schema.routines
WHERE 
    routine_type = 'FUNCTION'
AND
    routine_schema = 'public'
AND
    routine_name = %s
""", [name])
        return bool(list(fetch(cur)))


def base_schema(conn):
    """
    Handles the concrete tables.
    """
    with conn.cursor() as cur:
        if not TypeInfo.fetch(conn, "CIRCUIT_COLOR"):
            cur.execute("""CREATE TYPE CIRCUIT_COLOR AS ENUM ('red', 'green');""")

        # TODO: Handle upgrades and shit
        cur.execute("""
CREATE TABLE IF NOT EXISTS __surface__ (
    surface_index INTEGER NOT NULL PRIMARY KEY,
    automation_id INTEGER NULL,
    name TEXT NULL
);

CREATE TABLE IF NOT EXISTS __raw__ (
    stamp INTERVAL NOT NULL,  -- time in seconds relative to game epoch
    name TEXT NOT NULL,
    tags HSTORE DEFAULT '',
    surface_index INTEGER NOT NULL,  -- soft foreign key to __surfaces__
    color CIRCUIT_COLOR NOT NULL,
    data HSTORE NOT NULL
);

CREATE INDEX ON __raw__ (name);
CREATE INDEX ON __raw__ (stamp);
""")

        if not _has_func(conn, 'game_epoch'):
            set_epoch(conn, datetime.datetime.now())


def set_epoch(conn, time: datetime.datetime):
    with conn.cursor() as cur:
        cur.execute(f"""
CREATE OR REPLACE FUNCTION game_epoch() RETURNS timestamp
IMMUTABLE LANGUAGE SQL AS $$
SELECT '{time.isoformat()}'::TIMESTAMP
$$
""")


def _read_view_columns(cur) -> dict:
    cur.execute("""
SELECT t.table_schema as schema_name,
       t.table_name as view_name,
       c.column_name,
       case when c.character_maximum_length is not null
            then c.character_maximum_length
            else c.numeric_precision end as max_length,
       is_nullable
    from information_schema.tables t
        left join information_schema.columns c 
              on t.table_schema = c.table_schema 
              and t.table_name = c.table_name
where table_type = 'VIEW' 
      -- and t.table_schema not in ('information_schema', 'pg_catalog')
      and t.table_schema = 'public'
order by schema_name,
         view_name;
""")
    columns = {}
    for row in fetch(cur):
        key = row.view_name
        if key not in columns:
            columns[key] = set()
        columns[key].add(row.column_name)
    return columns


def _create_view(cur, name, kinds):
    print(f"{kinds=}")
    assert kinds
    columns = SQL(', \n').join(
        SQL("(__raw__.data -> {lkind})::INTEGER AS {ikind}").format(
            lkind=Literal(kind), 
            ikind=Identifier(kind),
        )
        for kind in kinds
    )
    print(f"{columns=}")
    q = SQL("""
CREATE OR REPLACE VIEW {iname} AS
SELECT 
    (game_epoch() + __raw__.stamp)::timestamp AS time, 
    __raw__.tags AS tags,
    __surface__.automation_id AS surface_id,
    __surface__.name AS surface_name,
    {columns}
FROM __raw__ LEFT JOIN __surface__ ON __raw__.surface_index = __surface__.surface_index
WHERE __raw__.name = {lname};
""").format(
        columns=columns,
        iname=Identifier(name),
        lname=Literal(name),
    )
    print(f"{q=}")
    print(q.as_bytes(cur))
    cur.execute(q)


def check_view_columns(conn, kinds):
    """
    Ensures that any defined views have all the needed columns.
    """
    kinds = set(kinds)
    with conn.cursor() as cur:
        views = _read_view_columns(cur)
        for view, cols in views.items():
            cols -= {
                # The set of standard view columns
                'time', 'tags', 'suface_id', 'surface_name',
            }
            if cols ^ kinds:
                # The set of columns has differed
                cur.execute(f"""
DROP VIEW "{view}"
""")
                _create_view(conn, view, kinds)


def check_view_names(conn, names, kinds):
    """
    Ensures there's a view for each stat name.

    This does not check view contents, just existance.
    """
    with conn.cursor() as cur:
        views = _read_view_columns(cur)  # FIXME: Use a much simpler query.
        for name in set(names) - set(views.keys()):
            _create_view(cur, name, kinds)
