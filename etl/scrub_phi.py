# etl/scrub_phi.py
import re
import csv
import json
import hashlib
import shutil
from pathlib import Path
from datetime import datetime

from etl.db import PostgresDB
from etl.provenance_recorder import ProvenanceRecorder

RAW_ZONE = Path("data/raw")
CURATED_ZONE = Path("data/curated")
# ensure curated zone exists
CURATED_ZONE.mkdir(parents=True, exist_ok=True)

class PhiRule:
    def __init__(self, rule_id, pattern, replacement, description):
        self.rule_id = rule_id
        self.pattern = pattern
        self.replacement = replacement
        self.description = description
        # compile with flags for case-insensitive where appropriate
        try:
            self.compiled = re.compile(pattern, flags=re.IGNORECASE)
        except Exception:
            self.compiled = re.compile(re.escape(pattern))

    def apply_to_text(self, text):
        """Apply regex replacement and return (new_text, applied_flag)."""
        if not text:
            return text, False
        new_text, count = self.compiled.subn(self.replacement, text)
        return new_text, (count > 0)

class Scrubber:

    @staticmethod
    def load_phi_rules():
        q = "SELECT rule_id, pattern, replacement, description FROM phi_rules ORDER BY rule_id"
        rows = PostgresDB.execute(q, fetch=True)
        rules = [PhiRule(r[0], r[1], r[2], r[3]) for r in (rows or [])]
        return rules

    @staticmethod
    def fetch_schema(source_name):
        q = """
        SELECT column_name, data_type, is_nullable, is_phi, phi_rule
        FROM schema_metadata
        WHERE source_name = %s
        ORDER BY id ASC
        """
        rows = PostgresDB.execute(q, (source_name,), fetch=True)
        meta = []
        for r in (rows or []):
            meta.append({
                "column_name": r[0],
                "data_type": r[1],
                "is_nullable": r[2],
                "is_phi": r[3],
                "phi_rule": r[4]
            })
        return meta

    @staticmethod
    def compute_sha256(file_path: Path):
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def scrub_csv(batch_id, source_name, raw_path, meta_rows, phi_rules):
        raw_path = Path(raw_path)
        target_dir = CURATED_ZONE / source_name
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / raw_path.name

        rules_applied = set()
        total_replacements = 0
        row_count = 0

        with open(raw_path, "r", encoding="utf-8", newline='') as rf, \
             open(out_path, "w", encoding="utf-8", newline='') as wf:
            reader = csv.DictReader(rf)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(wf, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                row_count += 1
                # column-level phi: replace entire column if marked is_phi
                for col_meta in meta_rows:
                    col = col_meta["column_name"]
                    if col not in row:
                        continue
                    if col_meta["is_phi"]:
                        # contextual replacement for names/dobs etc.
                        # for names use [REDACTED_NAME]; for dates [REDACTED_DATE]; else generic
                        dtype = (col_meta.get("data_type") or "").lower()
                        if "name" in col.lower() or "patient_name" in col.lower():
                            newval = "[REDACTED_NAME]"
                            row[col] = newval
                            rules_applied.add("PHI_COLUMN_REDACT_NAME")
                            total_replacements += 1
                        elif "dob" in col.lower() or "date" in dtype:
                            row[col] = "[REDACTED_DATE]"
                            rules_applied.add("PHI_COLUMN_REDACT_DATE")
                            total_replacements += 1
                        else:
                            row[col] = "[REDACTED]"
                            rules_applied.add("PHI_COLUMN_REDACT_GENERIC")
                            total_replacements += 1
                    else:
                        # apply global regex rules on non-PHI columns too (e.g., notes/free_text)
                        val = row.get(col)
                        if val:
                            for rule in phi_rules:
                                newval, applied = rule.apply_to_text(val)
                                if applied:
                                    row[col] = newval
                                    rules_applied.add(rule.rule_id)
                                    total_replacements += 1
                writer.writerow(row)

        curated_sha = Scrubber.compute_sha256(out_path)
        # provenance updates
        ProvenanceRecorder.update_curated_hash(batch_id, curated_sha)
        ProvenanceRecorder.record_step(batch_id, "SCRUB_PHI", {"curated_path": str(out_path.as_posix()), "rows": row_count, "replacements": total_replacements})
        for rid in rules_applied:
            ProvenanceRecorder.record_rule(batch_id, rid, "applied")
        ProvenanceRecorder.update_status(batch_id, "SCRUBBED")
        return str(out_path), curated_sha

    @staticmethod
    def scrub_jsonl(batch_id, source_name, raw_path, meta_rows, phi_rules):
        raw_path = Path(raw_path)
        target_dir = CURATED_ZONE / source_name
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / raw_path.name

        rules_applied = set()
        total_replacements = 0
        row_count = 0

        phi_columns = {m["column_name"] for m in meta_rows if m["is_phi"]}
        # open line-by-line
        with open(raw_path, "r", encoding="utf-8") as rf, open(out_path, "w", encoding="utf-8") as wf:
            for line in rf:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                row_count += 1
                try:
                    obj = json.loads(line)
                except Exception:
                    # keep invalid lines as-is but log
                    wf.write(line + "\n")
                    continue

                # redact column-level PHI
                for col in list(obj.keys()):
                    if col in phi_columns:
                        # simple heuristics
                        if "name" in col.lower():
                            obj[col] = "[REDACTED_NAME]"
                            rules_applied.add("PHI_COLUMN_REDACT_NAME")
                            total_replacements += 1
                        elif "dob" in col.lower() or "date" in col.lower():
                            obj[col] = "[REDACTED_DATE]"
                            rules_applied.add("PHI_COLUMN_REDACT_DATE")
                            total_replacements += 1
                        else:
                            obj[col] = "[REDACTED]"
                            rules_applied.add("PHI_COLUMN_REDACT_GENERIC")
                            total_replacements += 1
                    else:
                        # apply regex rules to textual fields
                        val = obj.get(col)
                        if isinstance(val, str) and val:
                            for rule in phi_rules:
                                newval, applied = rule.apply_to_text(val)
                                if applied:
                                    obj[col] = newval
                                    rules_applied.add(rule.rule_id)
                                    total_replacements += 1
                wf.write(json.dumps(obj) + "\n")

        curated_sha = Scrubber.compute_sha256(out_path)
        ProvenanceRecorder.update_curated_hash(batch_id, curated_sha)
        ProvenanceRecorder.record_step(batch_id, "SCRUB_PHI", {"curated_path": str(out_path.as_posix()), "rows": row_count, "replacements": total_replacements})
        for rid in rules_applied:
            ProvenanceRecorder.record_rule(batch_id, rid, "applied")
        ProvenanceRecorder.update_status(batch_id, "SCRUBBED")
        return str(out_path), curated_sha

    @staticmethod
    def scrub_hl7(batch_id, source_name, raw_path, meta_rows, phi_rules):
        raw_path = Path(raw_path)
        target_dir = CURATED_ZONE / source_name
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / raw_path.name

        # determine PID positions to redact from metadata
        pid_positions = []
        for m in meta_rows:
            cname = m["column_name"]
            if cname.upper().startswith("PID-"):
                try:
                    pos = int(cname.split("-")[1])
                    pid_positions.append((pos, m))
                except Exception:
                    pass

        rules_applied = set()
        total_replacements = 0
        msg_count = 0

        with open(raw_path, "r", encoding="utf-8") as rf, open(out_path, "w", encoding="utf-8") as wf:
            content = rf.read()
            messages = [msg.strip() for msg in content.split("\n\n") if msg.strip()]
            for idx, msg in enumerate(messages, start=1):
                msg_count += 1
                lines = msg.splitlines()
                new_lines = []
                for line in lines:
                    if line.startswith("PID|"):
                        parts = line.split("|")
                        # For each requested PID position, replace if present
                        for pos, meta in pid_positions:
                            # pos corresponds to HL7 field number (1-based after PID label)
                            # parts index equals pos (since parts[0] is "PID")
                            if pos < len(parts) and parts[pos].strip() != "":
                                # decide replacement token based on meta
                                colname = meta["column_name"].upper()
                                if "NAME" in colname or "PID-5" in colname:
                                    parts[pos] = "[REDACTED_NAME]"
                                    rules_applied.add("PHI_COLUMN_REDACT_NAME")
                                elif "DOB" in colname or "PID-7" in colname:
                                    parts[pos] = "[REDACTED_DATE]"
                                    rules_applied.add("PHI_COLUMN_REDACT_DATE")
                                else:
                                    # try applying regex rules to this part as well
                                    val = parts[pos]
                                    applied_any = False
                                    for rule in phi_rules:
                                        newval, applied = rule.apply_to_text(val)
                                        if applied:
                                            parts[pos] = newval
                                            rules_applied.add(rule.rule_id)
                                            applied_any = True
                                    if not applied_any:
                                        parts[pos] = "[REDACTED]"
                                        rules_applied.add("PHI_COLUMN_REDACT_GENERIC")
                                total_replacements += 1
                            else:
                                # missing field; nothing to do
                                pass
                        new_lines.append("|".join(parts))
                    else:
                        # apply regex rules to text lines as well (OBX, NTE)
                        new_line = line
                        for rule in phi_rules:
                            new_line, applied = rule.apply_to_text(new_line)
                            if applied:
                                rules_applied.add(rule.rule_id)
                                total_replacements += 1
                        new_lines.append(new_line)
                wf.write("\n".join(new_lines) + "\n\n")

        curated_sha = Scrubber.compute_sha256(out_path)
        ProvenanceRecorder.update_curated_hash(batch_id, curated_sha)
        ProvenanceRecorder.record_step(batch_id, "SCRUB_PHI", {"curated_path": str(out_path.as_posix()), "messages": msg_count, "replacements": total_replacements})
        for rid in rules_applied:
            ProvenanceRecorder.record_rule(batch_id, rid, "applied")
        ProvenanceRecorder.update_status(batch_id, "SCRUBBED")
        return str(out_path), curated_sha

    @staticmethod
    def run_scrub_on_validated():
        """
        Find batches with status VALIDATED and scrub them.
        """
        q = """
        SELECT batch_id, source_name, raw_file_path
        FROM provenance_batch
        WHERE status = 'VALIDATED'
        ORDER BY ingest_time ASC
        """
        rows = PostgresDB.execute(q, fetch=True)
        if not rows:
            print("[SCRUB] No VALIDATED batches found.")
            return []

        phi_rules = Scrubber.load_phi_rules()
        processed = []
        for r in rows:
            batch_id, source_name, raw_path = r[0], r[1], r[2]
            print(f"[SCRUB] Processing {batch_id} source={source_name}")
            try:
                meta = Scrubber.fetch_schema(source_name)
                if source_name.startswith("hospital_c_hl7") or source_name.endswith("_hl7") or source_name == "hospital_c_hl7":
                    out, sha = Scrubber.scrub_hl7(batch_id, source_name, raw_path, meta, phi_rules)
                else:
                    # determine from source_registry type to be safe
                    sr = PostgresDB.execute("SELECT source_type FROM source_registry WHERE source_name=%s", (source_name,), fetch=True)
                    s_type = sr[0][0] if sr else "csv"
                    if s_type.lower() in ("jsonl", "json"):
                        out, sha = Scrubber.scrub_jsonl(batch_id, source_name, raw_path, meta, phi_rules)
                    else:
                        out, sha = Scrubber.scrub_csv(batch_id, source_name, raw_path, meta, phi_rules)
                processed.append({"batch_id": batch_id, "curated": out, "sha": sha})
            except Exception as e:
                print(f"[SCRUB] Error scrubbing {batch_id}: {e}")
                ProvenanceRecorder.record_step(batch_id, "SCRUB_ERROR", {"exception": str(e)})
                ProvenanceRecorder.update_status(batch_id, "FAILED_SCRUB", error_details=str(e))
        return processed
