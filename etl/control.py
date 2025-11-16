# etl/control.py
from etl.db import PostgresDB
import json


class Control:

    @staticmethod
    def get_pending_controls():
        query = """
        SELECT control_id, source_name
        FROM control_header
        WHERE status = 'PENDING'
        ORDER BY scheduled_time ASC
        """
        return PostgresDB.execute(query, fetch=True)

    @staticmethod
    def get_control_steps(control_id):
        query = """
        SELECT step_order, step_type, config_json
        FROM control_detail
        WHERE control_id = %s
        ORDER BY step_order ASC
        """
        rows = PostgresDB.execute(query, (control_id,), fetch=True)
        return [
            {
                "step_order": r[0],
                "step_type": r[1],
                "config": r[2]
            }
            for r in rows
        ]

    @staticmethod
    def mark_control_status(control_id, status):
        query = """
        UPDATE control_header
        SET status = %s, run_time = NOW()
        WHERE control_id = %s
        """
        PostgresDB.execute(query, (status, control_id))

    @staticmethod
    def fetch_source_metadata(source_name):
        query = """
        SELECT column_name, data_type, is_nullable, is_phi, phi_rule
        FROM schema_metadata
        WHERE source_name = %s
        ORDER BY id ASC
        """
        rows = PostgresDB.execute(query, (source_name,), fetch=True)
        return [
            {
                "column_name": r[0],
                "data_type": r[1],
                "is_nullable": r[2],
                "is_phi": r[3],
                "phi_rule": r[4]
            }
            for r in rows
        ]
