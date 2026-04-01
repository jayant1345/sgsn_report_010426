# db.py
# Database connection helpers for PostgreSQL and MySQL

import psycopg2
import psycopg2.extras
import mysql.connector
from config import (PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS,
                    MY_HOST, MY_PORT, MY_DB, MY_USER, MY_PASS)


def pg_connect():
    """Return a PostgreSQL connection to NetAct NOC database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        connect_timeout=30
    )


def pg_query(sql, params=None):
    """Execute a SELECT on PostgreSQL and return list of dicts."""
    conn = pg_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def pg_execute(sql, params=None):
    """Execute an INSERT/DELETE/UPDATE on PostgreSQL."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def pg_execute_many(sql, data):
    """
    Bulk insert into PostgreSQL using executemany (cursor.executemany).
    SQL must use %s placeholders for each column, e.g.:
      INSERT INTO t (a,b,c) VALUES (%s, %s, %s)
    data = list of tuples, one per row.
    Uses executemany instead of execute_values to avoid the
    'more than one %s placeholder' error from execute_values.
    """
    if not data:
        return
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
    finally:
        conn.close()


def my_connect():
    """Return a MySQL connection to reportsDB."""
    return mysql.connector.connect(
        host=MY_HOST, port=MY_PORT, database=MY_DB,
        user=MY_USER, password=MY_PASS,
        connection_timeout=30
    )


def my_query(sql, params=None):
    """Execute a SELECT on MySQL and return list of dicts."""
    conn = my_connect()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def my_execute(sql, params=None):
    """Execute a single statement on MySQL."""
    conn = my_connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def my_execute_many(sql, data):
    """Bulk insert into MySQL."""
    conn = my_connect()
    try:
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
    finally:
        conn.close()
