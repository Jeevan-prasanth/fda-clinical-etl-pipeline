# tests/test_db_connection.py
from etl.db import PostgresDB

def test_connection():
    PostgresDB.initialize()
    rows = PostgresDB.execute("SELECT NOW();", fetch=True)
    print("Connection OK:", rows)

test_connection()