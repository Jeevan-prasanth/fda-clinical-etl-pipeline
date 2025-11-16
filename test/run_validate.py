# run_validate.py
from etl.db import PostgresDB
from etl.validate import Validator
import json


def write_audit(actor, action, batch_id=None, details=None, severity="INFO"):
    PostgresDB.execute(
        """
        INSERT INTO audit_log(actor, action, batch_id, details, severity)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (actor, action, batch_id, json.dumps(details or {}), severity)
    )


if __name__ == "__main__":
    PostgresDB.initialize()

    try:
        results = Validator.run_pending_validations()
        for r in results:
            write_audit("etl_runner", "validation", r["batch_id"], {"ok": r["ok"]})
    except Exception as e:
        write_audit("etl_runner", "validation_failed", None, {"error": str(e)}, "ERROR")
        raise
    print("Validation results:")
    for r in results:
        print(r)
