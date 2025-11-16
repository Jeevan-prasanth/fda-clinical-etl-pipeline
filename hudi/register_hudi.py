import sys
import json
from pathlib import Path
from hudi.spark_session import get_spark
from pyspark.sql import functions as F


def find_latest_parquet(source):
    """
    Auto-detect the latest timestamp folder and latest parquet file.
    """
    base = Path("data/qlm_ready") / source

    if not base.exists():
        raise FileNotFoundError(f"[ERROR] Source folder not found: {base}")

    # Find all timestamp directories
    ts_dirs = [d for d in base.iterdir() if d.is_dir()]

    if not ts_dirs:
        raise FileNotFoundError(f"[ERROR] No timestamp folders found under {base}")

    # Sort timestamp folders by name descending -> newest first
    ts_dirs_sorted = sorted(ts_dirs, key=lambda p: p.name, reverse=True)
    latest_folder = ts_dirs_sorted[0]

    print(f"[INFO] Latest folder detected: {latest_folder}")

    # Find parquet files inside
    parquet_files = list(latest_folder.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"[ERROR] No parquet files found inside {latest_folder}")

    # If multiple files exist, choose the newest by modified time
    parquet_files_sorted = sorted(parquet_files, key=lambda p: p.stat().st_mtime, reverse=True)
    latest_parquet = parquet_files_sorted[0]

    print(f"[INFO] Latest parquet file: {latest_parquet}")

    return latest_parquet


def register_hudi(source):
    """
    Register QLM-ready parquet as a Hudi versioned table.
    """
    spark = get_spark()

    parquet_path = find_latest_parquet(source)
    df = spark.read.parquet(str(parquet_path))

    table_path = str((Path.cwd() / "data" / "hudi" / source).resolve()).replace("\\", "/")


    hudi_options = {
    "hoodie.table.name": source,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",

    # Primary key
    "hoodie.datasource.write.recordkey.field": "patient_id",

    # Precombine (tie-break when duplicate keys)
    "hoodie.datasource.write.precombine.field": "visit_date",

    # No partitioning
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    }


    print(f"[INFO] Writing to Hudi table at: {table_path}")

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(table_path)

    print("[SUCCESS] Hudi write completed!")

    # Show commit history
    print("\n=== Hudi Commits ===")
    commits = spark.read.format("hudi").load(table_path) \
        .select("_hoodie_commit_time") \
        .distinct() \
        .orderBy("_hoodie_commit_time", ascending=False)

    commits.show(truncate=False)

    # Return commit times for provenance updates
    commit_list = commits.toPandas()["_hoodie_commit_time"].tolist()
    if commit_list:
        print(f"[INFO] Latest commit: {commit_list[0]}")
        return commit_list[0]

    return None


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python register_hudi.py <source_name>")
        sys.exit(1)

    latest_commit = register_hudi(sys.argv[1])

    print(f"[DONE] Latest Hudi Commit = {latest_commit}")
