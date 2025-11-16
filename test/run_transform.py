import json
from etl.db import PostgresDB
from etl.transform import run_transform_on_scrubbed

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
        results = run_transform_on_scrubbed()
        for r in results:
            write_audit(
                "etl_runner",
                "transform",
                r["batch_id"],
                {
                    "ok": True,
                    "curated_path": r.get("curated"),
                    "sha256": r.get("sha")
                }
            )

    except Exception as e:
        write_audit("etl_runner", "transform_failed", None, {"error": str(e)}, "ERROR")
        raise
