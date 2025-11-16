# etl/ingest.py
import os
import shutil
import hashlib
from datetime import datetime
from pathlib import Path

from etl.db import PostgresDB
from etl.provenance_recorder import ProvenanceRecorder


RAW_ZONE = Path("data/raw")
SOURCE_ROOT = Path("data_source")


class IngestPipeline:

    @staticmethod
    def compute_sha256(file_path):
        """Compute SHA256 for a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def load_source_registry():
        """Load active sources."""
        query = """
        SELECT source_name, source_type, file_path
        FROM source_registry
        WHERE active = TRUE
        """
        rows = PostgresDB.execute(query, fetch=True)
        return [
            {
                "source_name": r[0],
                "source_type": r[1],
                "file_path": r[2]
            }
            for r in rows
        ]

    @staticmethod
    def build_batch_id(source_name, filename):
        """Generate batch_id including timestamp."""
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        clean = filename.replace(".", "_")
        return f"{source_name}_{clean}_{ts}"

    @staticmethod
    def ingest_sources():
        """Copy source files into RAW zone and record provenance."""
        sources = IngestPipeline.load_source_registry()

        created_batches = []

        for src in sources:
            source_name = src["source_name"]
            source_path = Path(src["file_path"])

            if not source_path.exists():
                print(f"[WARN] Source folder not found: {source_path}")
                continue

            # RAW/<source>/ folder
            raw_dest_dir = RAW_ZONE / source_name
            raw_dest_dir.mkdir(parents=True, exist_ok=True)

            files = list(source_path.glob("*"))

            if not files:
                print(f"[INFO] No files found for source {source_name}")
                continue

            for file in files:
                print(f"[INGEST] Processing file: {file}")

                # Compute raw hash from SOURCE file
                sha = IngestPipeline.compute_sha256(file)

                # Build batch_id
                batch_id = IngestPipeline.build_batch_id(source_name, file.name)

                # Destination in RAW zone
                raw_dest = raw_dest_dir / file.name

                # COPY file → RAW (not move!)
                shutil.copy2(str(file), str(raw_dest))

                # Save RAW path in provenance (IMPORTANT)
                raw_path_clean = str(raw_dest.as_posix())

                # Insert batch with raw path
                ProvenanceRecorder.create_batch(
                    batch_id=batch_id,
                    source_name=source_name,
                    raw_file_path=raw_path_clean,
                    raw_sha256=sha
                )

                # Insert INGEST step
                ProvenanceRecorder.record_step(
                    batch_id=batch_id,
                    step_name="INGEST",
                    details={"source_file": str(file), "raw_copy": raw_path_clean, "sha256": sha}
                )

                print(f"[INGEST] Copied to RAW: {raw_dest}")

                created_batches.append(batch_id)

        return created_batches
