# etl/validate.py
import os
import shutil
import csv
import json
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse as dateparse
from jsonschema import validate as jsonschema_validate, ValidationError
from etl.db import PostgresDB
from etl.provenance_recorder import ProvenanceRecorder

# Zones
RAW_ZONE = Path("data/raw")
QUARANTINE_ZONE = Path("data/quarantine")
PROCESSING_ZONE = Path("data/processing")

# Simple mapping of metadata types to validators
def _validate_type(value, dtype):
    if value is None or value == "":
        return True  # Nullability handled separately by metadata
    try:
        if dtype.lower() in ("int", "integer"):
            int(value)
        elif dtype.lower() in ("float", "double", "numeric"):
            float(value)
        elif dtype.lower() in ("date", "datetime", "timestamp"):
            dateparse(value)
        elif dtype.lower() in ("string", "text"):
            # always ok
            pass
        else:
            # unknown => accept (you can extend)
            pass
        return True
    except Exception:
        return False

class Validator:

    @staticmethod
    def get_pending_batches():
        """Return provenance_batch rows with status 'INGESTED'"""
        q = """
        SELECT batch_id, source_name, raw_file_path
        FROM provenance_batch
        WHERE status = 'INGESTED'
        ORDER BY ingest_time ASC
        """
        return PostgresDB.execute(q, fetch=True)

    @staticmethod
    def fetch_schema_metadata(source_name):
        """Fetch schema metadata rows for a source"""
        q = """
        SELECT column_name, data_type, is_nullable, is_phi
        FROM schema_metadata
        WHERE source_name = %s
        ORDER BY id ASC
        """
        rows = PostgresDB.execute(q, (source_name,), fetch=True)
        return [
            {"column_name": r[0], "data_type": r[1], "is_nullable": r[2], "is_phi": r[3]}
            for r in rows
        ]

    @staticmethod
    def build_jsonschema_from_metadata(meta_rows):
        """
        Build a simple JSON Schema dict for jsonschema library.
        Only uses 'required' and type hints (string, integer, number).
        """
        schema = {"type": "object", "properties": {}, "required": []}
        for col in meta_rows:
            cname = col["column_name"]
            dtype = col["data_type"].lower() if col["data_type"] else "string"
            # map to json schema types
            if "int" in dtype:
                jtype = "integer"
            elif "float" in dtype or "double" in dtype or "numeric" in dtype:
                jtype = "number"
            elif "bool" in dtype:
                jtype = "boolean"
            else:
                jtype = "string"
            schema["properties"][cname] = {"type": jtype}
            if not col["is_nullable"]:
                schema["required"].append(cname)
        return schema

    @staticmethod
    def validate_csv(file_path: Path, meta_rows):
        """Validate CSV columns and simple per-column types; return (ok, details, row_count)."""
        details = {"errors": []}
        expected_cols = [c["column_name"] for c in meta_rows]
        expected_map = {c["column_name"]: c for c in meta_rows}

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            actual_cols = reader.fieldnames or []
            # Check columns presence
            missing = [c for c in expected_cols if c not in actual_cols]
            extra = [c for c in actual_cols if c not in expected_cols]
            if missing:
                details["errors"].append({"type": "missing_columns", "columns": missing})
            # Type checking sample rows (first 100 rows)
            row_count = 0
            type_errors = []
            for r_idx, row in enumerate(reader, start=1):
                row_count += 1
                # sample up to 200 rows to avoid big cost
                if r_idx <= 200:
                    for col, meta in expected_map.items():
                        val = row.get(col, "")
                        if val == "" and (not meta["is_nullable"]):
                            type_errors.append({"row": r_idx, "col": col, "error": "null_not_allowed"})
                        else:
                            if val != "" and not _validate_type(val, meta["data_type"]):
                                type_errors.append({"row": r_idx, "col": col, "error": f"type_mismatch({meta['data_type']})"})
                # no early exit, continue to get exact row_count
            details["row_count"] = row_count
            if type_errors:
                details["errors"].append({"type": "type_errors", "sample": type_errors[:20]})
        ok = len(details["errors"]) == 0
        return ok, details, row_count

    @staticmethod
    def validate_jsonl(file_path: Path, meta_rows):
        """Validate JSONL records against schema built from metadata."""
        schema = Validator.build_jsonschema_from_metadata(meta_rows)
        details = {"errors": []}
        row_count = 0
        sample_errors = []
        with open(file_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                row_count += 1
                try:
                    obj = json.loads(line)
                except Exception as e:
                    details["errors"].append({"row": idx, "error": "invalid_json", "detail": str(e)})
                    if len(sample_errors) < 20:
                        sample_errors.append({"row": idx, "error": str(e)})
                    continue
                try:
                    jsonschema_validate(instance=obj, schema=schema)
                except ValidationError as ve:
                    details["errors"].append({"row": idx, "error": "schema_mismatch", "detail": str(ve.message)})
                    if len(sample_errors) < 20:
                        sample_errors.append({"row": idx, "error": str(ve.message)})
                # sample performance: optionally stop validating after some rows? here we validate all
        if sample_errors:
            details["sample_errors"] = sample_errors
        ok = len(details["errors"]) == 0
        details["row_count"] = row_count
        return ok, details, row_count

    @staticmethod
    def validate_hl7(file_path: Path, meta_rows):
        """
        HL7 validation:
         Ensures PID segment exists
         Checks field positions correctly (PID-3 means index 3)
         Does NOT enforce strict HL7 rules beyond what's needed
        """
        details = {"errors": []}
        row_count = 0

        # Collect required PID positions from schema_metadata
        required_pid_fields = []
        for m in meta_rows:
            cname = m["column_name"]
            if cname.upper().startswith("PID-"):
                required_pid_fields.append(int(cname.split("-")[1]))


        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Split messages
        messages = [msg.strip() for msg in content.split("\n\n") if msg.strip()]
        row_count = len(messages)

        missing_list = []

        for idx, msg in enumerate(messages, start=1):
            lines = msg.splitlines()
            pid_lines = [l for l in lines if l.startswith("PID|")]

            if not pid_lines:
                missing_list.append({"msg_index": idx, "error": "PID segment missing"})
                continue

            pid = pid_lines[0]
            parts = pid.split("|")  # HL7 fields begin at index 0: PID, 1, 2, 3...

            for field_position in required_pid_fields:
                # Example: PID-3 → check index 3
                if field_position >= len(parts) or parts[field_position].strip() == "":
                    missing_list.append({
                        "msg_index": idx,
                        "missing_field": f"PID-{field_position}"
                    })

        if missing_list:
            details["errors"].append({"type": "missing_fields", "sample": missing_list[:20]})

        ok = len(missing_list) == 0
        details["row_count"] = row_count

        return ok, details, row_count


    @staticmethod
    def quarantine_file(src_path: Path, source_name: str):
        """Move file to quarantine zone under the source_name"""
        target_dir = QUARANTINE_ZONE / source_name
        target_dir.mkdir(parents=True, exist_ok=True)
        dest = target_dir / src_path.name
        shutil.move(str(src_path), str(dest))
        return dest

    @staticmethod
    def process_batch(batch_id, source_name, raw_file_path):
        """
        Validate a single batch's raw file.
        Updates provenance_batch and provenance_steps accordingly.
        """
        file_path = Path(raw_file_path)
        if not file_path.exists():
            # Might have already been moved; record failure
            ProvenanceRecorder.record_step(batch_id, "VALIDATION_FAILED", {"error": "raw_file_missing", "path": raw_file_path})
            ProvenanceRecorder.update_status(batch_id, "FAILED_VALIDATION", error_details="raw_file_missing")
            return False
        # Load schema metadata
        meta = Validator.fetch_schema_metadata(source_name)
        if not meta:
            # No schema provided: treat as warning but pass
            ProvenanceRecorder.record_step(batch_id, "VALIDATION_SKIPPED", {"reason": "no_schema_metadata"})
            ProvenanceRecorder.update_status(batch_id, "VALIDATED")
            return True

        # Branch by source type (try to infer from source_registry, but schema_metadata usage implies known type)
        # We'll fetch source_type from source_registry
        q = "SELECT source_type FROM source_registry WHERE source_name = %s"
        rows = PostgresDB.execute(q, (source_name,), fetch=True)
        source_type = rows[0][0] if rows else "csv"

        try:
            if source_type.lower() == "csv":
                ok, details, row_count = Validator.validate_csv(file_path, meta)
            elif source_type.lower() == "jsonl":
                ok, details, row_count = Validator.validate_jsonl(file_path, meta)
            elif source_type.lower() in ("hl7", "hl7v2"):
                ok, details, row_count = Validator.validate_hl7(file_path, meta)
            else:
                # default: try JSONL
                ok, details, row_count = Validator.validate_jsonl(file_path, meta)
        except Exception as e:
            # unexpected error during validation
            ProvenanceRecorder.record_step(batch_id, "VALIDATION_ERROR", {"exception": str(e)})
            ProvenanceRecorder.update_status(batch_id, "FAILED_VALIDATION", error_details=str(e))
            return False

        if not ok:
            # Move to quarantine and update provenance
            dest = Validator.quarantine_file(file_path, source_name)
            ProvenanceRecorder.record_step(batch_id, "VALIDATION_FAILED", details)
            ProvenanceRecorder.update_status(batch_id, "FAILED_VALIDATION", error_details=json.dumps(details))
            # Optionally record where quarantine copy is
            ProvenanceRecorder.record_step(batch_id, "QUARANTINE_MOVED", {"quarantine_path": str(dest)})
            return False
        else:
            # Validation passed
            ProvenanceRecorder.record_step(batch_id, "VALIDATION_PASSED", details)
            # Update row count and status
            q_up = """
            UPDATE provenance_batch
            SET status = 'VALIDATED', total_rows = %s
            WHERE batch_id = %s
            """
            PostgresDB.execute(q_up, (details.get("row_count", row_count), batch_id))
            return True

    @staticmethod
    def run_pending_validations():
        """Find all INGESTED batches and validate them."""
        rows = Validator.get_pending_batches()
        if not rows:
            print("[VALIDATE] No INGESTED batches found.")
            return []

        processed = []
        for r in rows:
            batch_id, source_name, raw_file_path = r[0], r[1], r[2]
            print(f"[VALIDATE] Validating batch {batch_id} (source={source_name})")
            try:
                ok = Validator.process_batch(batch_id, source_name, raw_file_path)
                processed.append({"batch_id": batch_id, "ok": ok})
            except Exception as e:
                print(f"[VALIDATE] Error validating {batch_id}: {e}")
                ProvenanceRecorder.record_step(batch_id, "VALIDATION_EXCEPTION", {"exception": str(e)})
                ProvenanceRecorder.update_status(batch_id, "FAILED_VALIDATION", error_details=str(e))
        return processed
