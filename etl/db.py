# etl/db.py
import psycopg2
from psycopg2 import pool
import yaml
import os
import time

class PostgresDB:
    _pool = None

    @staticmethod
    def initialize(db_config_path="config/db_config.yml"):
        """Initialize connection pool"""
        with open(db_config_path, "r") as f:
            cfg = yaml.safe_load(f)

        for attempt in range(5):
            try:
                PostgresDB._pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    user=cfg["user"],
                    password=cfg["password"],
                    host=cfg["host"],
                    port=cfg["port"],
                    database=cfg["database"]
                )
                print("Postgres connection pool initialized.")
                return
            except Exception as e:
                print(f"DB connection failed. Retry {attempt+1}/5. Error: {e}")
                time.sleep(2)

        raise Exception("Failed to initialize DB connection pool.")

    @staticmethod
    def get_conn():
        if PostgresDB._pool is None:
            raise Exception("DB pool not initialized!")
        return PostgresDB._pool.getconn()

    @staticmethod
    def release_conn(conn):
        if PostgresDB._pool:
            PostgresDB._pool.putconn(conn)

    @staticmethod
    def execute(query, params=None, fetch=False):
        conn = PostgresDB.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            PostgresDB.release_conn(conn)
