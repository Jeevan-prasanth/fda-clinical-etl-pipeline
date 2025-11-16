# etl/provenance_recorder.py
import json
from datetime import datetime
from etl.db import PostgresDB


class ProvenanceRecorder:

    @staticmethod
    def create_batch(batch_id, source_name, raw_file_path, raw_sha256):
        query = """
        INSERT INTO provenance_batch 
        (batch_id, source_name, raw_file_path, raw_sha256, status)
        VALUES (%s, %s, %s, %s, 'INGESTED')
        ON CONFLICT (batch_id) DO NOTHING;
        """
        PostgresDB.execute(query, (batch_id, source_name, raw_file_path, raw_sha256))

    @staticmethod
    def update_status(batch_id, status, error_details=None):
        query = """
        UPDATE provenance_batch
        SET status = %s,
            error_details = %s
        WHERE batch_id = %s
        """
        PostgresDB.execute(query, (status, error_details, batch_id))

    @staticmethod
    def update_curated_hash(batch_id, curated_sha256):
        query = """
        UPDATE provenance_batch
        SET curated_sha256 = %s
        WHERE batch_id = %s
        """
        PostgresDB.execute(query, (curated_sha256, batch_id))

    @staticmethod
    def update_final_hash(batch_id, final_sha256, version_path):
        query = """
        UPDATE provenance_batch
        SET final_sha256 = %s,
            version_path = %s,
            status = 'COMPLETED'
        WHERE batch_id = %s
        """
        PostgresDB.execute(query, (final_sha256, version_path, batch_id))

    @staticmethod
    def record_step(batch_id, step_name, details: dict):
        query = """
        INSERT INTO provenance_steps 
        (batch_id, step_name, details_json)
        VALUES (%s, %s, %s)
        """
        PostgresDB.execute(query, (batch_id, step_name, json.dumps(details)))

    @staticmethod
    def record_rule(batch_id, rule_id, description):
        query = """
        INSERT INTO provenance_rules_applied 
        (batch_id, rule_id, description)
        VALUES (%s, %s, %s)
        """
        PostgresDB.execute(query, (batch_id, rule_id, description))
