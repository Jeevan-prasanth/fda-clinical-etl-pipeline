import json
from etl.db import PostgresDB

def write_audit(actor, action, batch_id=None, details=None, severity="INFO"):
    PostgresDB.execute(
        """
        INSERT INTO audit_log(actor, action, batch_id, details, severity)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (actor, action, batch_id, json.dumps(details or {}), severity)
    )
