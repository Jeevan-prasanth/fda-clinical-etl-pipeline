import json
from etl.db import PostgresDB
from etl.scrub_phi import Scrubber

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
        results = Scrubber.run_scrub_on_validated()
        for r in results:
            write_audit(
                "etl_runner",
                "scrub_phi",
                r["batch_id"],
                {
                    "ok": True,
                    "curated_path": r.get("curated"),
                    "sha256": r.get("sha")
                }
            )

    except Exception as e:
        write_audit("etl_runner", "scrub_phi_failed", None, {"error": str(e)}, "ERROR")
        raise
