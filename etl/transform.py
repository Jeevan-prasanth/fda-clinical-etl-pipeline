# etl/transform.py
import os
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.db import PostgresDB
from etl.provenance_recorder import ProvenanceRecorder

CURATED_ZONE = Path("data/curated")
QLM_ZONE = Path("data/qlm_ready")


def compute_sha256(file_path: Path):
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_table(df: pd.DataFrame, out_path: Path):
    """
    Write DataFrame to parquet if possible, else CSV.
    Returns final path written.
    """
    try:
        # Prefer parquet for column types and efficiency
        df.to_parquet(out_path, index=False)
        return out_path
    except Exception as e:
        # Fallback to CSV
        csv_path = out_path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        return csv_path


def canonicalize_hospital_a(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map hospital_a curated CSV to canonical QLM schema:
    patient_id, patient_name, dob, visit_date, diagnosis, note_text, source
    """
    out = pd.DataFrame()
    out["patient_id"] = df.get("patient_id")
    out["patient_name"] = df.get("patient_name")
    out["dob"] = df.get("dob")
    out["visit_date"] = df.get("visit_date")
    out["diagnosis"] = df.get("diagnosis")
    # notes -> note_text
    out["note_text"] = df.get("notes") if "notes" in df.columns else df.get("note")
    out["source"] = "hospital_a"
    return out


def canonicalize_clinic_b(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map clinic_b JSONL to canonical QLM schema:
    id->patient_id, name->patient_name, date_of_birth->dob, encounter->visit_date,
    icd->diagnosis (or diagnosis_code), free_text->note_text
    """
    out = pd.DataFrame()
    out["patient_id"] = df.get("id")
    out["patient_name"] = df.get("name")
    # unify name/dob column names
    out["dob"] = df.get("date_of_birth")
    out["visit_date"] = df.get("encounter")
    out["diagnosis"] = df.get("icd")
    out["note_text"] = df.get("free_text")
    out["source"] = "clinic_b"
    return out


def parse_hl7_messages_to_df(raw_path: Path) -> pd.DataFrame:
    """
    Parse curated HL7 file where messages are separated by blank lines.
    Extract PID fields into columns. Designed for the produced curated HL7 (redacted).
    The function extracts PID-n fields up to a reasonable limit and returns DataFrame.
    """
    msgs = []
    text = raw_path.read_text(encoding="utf-8")
    messages = [m.strip() for m in text.split("\n\n") if m.strip()]
    for msg in messages:
        pid_line = None
        for line in msg.splitlines():
            if line.startswith("PID|"):
                pid_line = line
                break
        record = {}
        if pid_line:
            parts = pid_line.split("|")
            # HL7 fields are 1-based after the segment; parts[0] == "PID"
            # Extract common fields that we used in schema_metadata
            def safe_get_field(pos):
                if pos < len(parts):
                    return parts[pos].strip()
                return None
            record["PID-3"] = safe_get_field(3)
            record["PID-5"] = safe_get_field(5)
            record["PID-7"] = safe_get_field(7)
            record["PID-11"] = safe_get_field(11)
            # some files had SSN in PID-16 or PID-17 depending on generator; try both
            record["PID-16"] = safe_get_field(16)
            record["PID-17"] = safe_get_field(17)
            record["PID-19"] = safe_get_field(19)
        # optional: also collect OBX as text
        obx_lines = [l for l in msg.splitlines() if l.startswith("OBX|")]
        record["OBX_TEXT"] = " ".join(obx_lines) if obx_lines else None
        msgs.append(record)
    return pd.DataFrame(msgs)


def canonicalize_hospital_c_hl7(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map parsed HL7 fields into canonical QLM schema.
    Prefer PID-3, PID-5, PID-7. For ssn we attempt PID-16->PID-17->PID-19.
    """
    out = pd.DataFrame()
    out["patient_id"] = df.get("PID-3")
    out["patient_name"] = df.get("PID-5")
    out["dob"] = df.get("PID-7")
    # address stored in PID-11 but HL7 components may be composite; treat as raw string
    out["address"] = df.get("PID-11")
    # ssn best guess
    ssn = df.get("PID-16").fillna("") if "PID-16" in df.columns else pd.Series([None]*len(df))
    if "PID-17" in df.columns:
        ssn = ssn.fillna(df.get("PID-17"))
    if "PID-19" in df.columns:
        ssn = ssn.fillna(df.get("PID-19"))
    out["ssn"] = ssn
    out["note_text"] = df.get("OBX_TEXT")
    out["source"] = "hospital_c_hl7"
    return out


def write_versioned_artifact(df: pd.DataFrame, source_name: str, batch_id: str):
    """
    Write canonical df to qlm_ready with versioning:
    data/qlm_ready/<source>/<YYYYMMDD_HHMMSS>_<batch_id>.parquet
    Returns final_path (Path).
    """
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    # directory per source
    dest_dir = QLM_ZONE / source_name / ts
    ensure_dir(dest_dir)
    # file name: <batch_id>.parquet
    candidate = dest_dir / f"{batch_id}.parquet"
    final_path = write_table(df, candidate)
    return final_path


def transform_one_batch(batch_id: str, source_name: str, raw_path: str):
    """
    Transform a single batch (curated file) into QLM-ready artifact.
    Updates provenance on success/failure.
    """
    try:
        raw = Path(raw_path)
        if not raw.exists():
            raise FileNotFoundError(f"curated file missing: {raw}")

        # Read curated data depending on source
        if source_name == "hospital_a":
            df = pd.read_csv(raw, dtype=str)
            canonical = canonicalize_hospital_a(df)
        elif source_name == "clinic_b":
            # JSON lines
            try:
                df = pd.read_json(raw, lines=True)
            except Exception:
                # fallback: read line by line
                rows = []
                for line in raw.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    rows.append(json.loads(line))
                df = pd.DataFrame(rows)
            canonical = canonicalize_clinic_b(df)
        elif source_name == "hospital_c_hl7":
            parsed = parse_hl7_messages_to_df(raw)
            canonical = canonicalize_hospital_c_hl7(parsed)
        else:
            # fallback: try CSV then JSON
            try:
                df = pd.read_csv(raw, dtype=str)
                canonical = df  # best-effort passthrough
            except Exception:
                try:
                    df = pd.read_json(raw, lines=True)
                    canonical = df
                except Exception:
                    raise RuntimeError("Unknown source and unreadable format")

        # Basic canonical cleaning: ensure consistent column set and types (strings)
        canonical = canonical.astype(object).where(pd.notnull(canonical), None)

        # Write versioned artifact
        final_path = write_versioned_artifact(canonical, source_name, batch_id)
        final_sha = compute_sha256(final_path)

        # Update provenance: final_sha, version_path, set status COMPLETED
        ProvenanceRecorder.update_final_hash(batch_id=batch_id, final_sha256=final_sha, version_path=str(final_path.as_posix()))
        ProvenanceRecorder.record_step(batch_id, "TRANSFORM", {"version_path": str(final_path.as_posix()), "rows": len(canonical)})
        return True, {"final_path": str(final_path.as_posix()), "final_sha256": final_sha}
    except Exception as e:
        ProvenanceRecorder.record_step(batch_id, "TRANSFORM_ERROR", {"exception": str(e)})
        ProvenanceRecorder.update_status(batch_id, "FAILED_TRANSFORM", error_details=str(e))
        return False, {"error": str(e)}


def run_transform_on_scrubbed():
    """
    Transform all batches with status SCRUBBED.
    """
    q = """
    SELECT batch_id, source_name, raw_file_path
    FROM provenance_batch
    WHERE status = 'SCRUBBED'
    ORDER BY ingest_time ASC
    """
    rows = PostgresDB.execute(q, fetch=True)
    if not rows:
        print("[TRANSFORM] No SCRUBBED batches found.")
        return []

    processed = []
    for r in rows:
        batch_id, source_name, raw_path = r[0], r[1], r[2]
        print(f"[TRANSFORM] Processing {batch_id} ({source_name})")
        ok, info = transform_one_batch(batch_id, source_name, raw_path)
        processed.append({"batch_id": batch_id, "ok": ok, "info": info})
        if not ok:
            print(f"[TRANSFORM] Failed {batch_id}: {info.get('error')}")
    return processed
