# api/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any

from etl.db import PostgresDB

app = FastAPI(
    title="QLM Provenance API",
    description="FDA-grade provenance and lineage tracking API for clinical ETL pipelines.",
    version="1.0.0"
)


# ---------- Response Models ----------

class BatchInfo(BaseModel):
    batch_id: str
    source_name: str
    ingest_time: str
    status: str
    raw_file_path: str
    raw_sha256: Optional[str]
    curated_sha256: Optional[str]
    final_sha256: Optional[str]
    version_path: Optional[str]
    error_details: Optional[str]


class StepInfo(BaseModel):
    step_name: str
    step_time: str
    details_json: Any


class RuleInfo(BaseModel):
    rule_id: str
    description: Optional[str]
    applied_time: str


# ---------- DB Helper ----------

def fetch_all(query: str, params=None):
    result = PostgresDB.execute(query, params=params, fetch=True)
    return result or []


# ---------- API Endpoints ----------


@app.on_event("startup")
def startup_event():
    PostgresDB.initialize()


@app.get("/health", tags=["System"])
def health_check():
    return {"status": "OK", "message": "API is running."}


# 1. Get batch summary
@app.get("/provenance/batch/{batch_id}", response_model=BatchInfo, tags=["Provenance"])
def get_batch(batch_id: str):
    q = """
        SELECT batch_id, source_name, ingest_time, status,
               raw_file_path, raw_sha256,
               curated_sha256, final_sha256, version_path,
               error_details
        FROM provenance_batch
        WHERE batch_id = %s
    """
    rows = fetch_all(q, (batch_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Batch ID not found")

    row = rows[0]
    return BatchInfo(
        batch_id=row[0],
        source_name=row[1],
        ingest_time=str(row[2]),
        status=row[3],
        raw_file_path=row[4],
        raw_sha256=row[5],
        curated_sha256=row[6],
        final_sha256=row[7],
        version_path=row[8],
        error_details=row[9]
    )


# 2. Get all steps for a batch
@app.get("/provenance/steps/{batch_id}", response_model=List[StepInfo], tags=["Provenance"])
def get_steps(batch_id: str):
    q = """
        SELECT step_name, step_time, details_json
        FROM provenance_steps
        WHERE batch_id = %s
        ORDER BY step_time ASC
    """
    rows = fetch_all(q, (batch_id,))
    return [StepInfo(step_name=r[0], step_time=str(r[1]), details_json=r[2]) for r in rows]


# 3. Get all PHI rules applied for a batch
@app.get("/provenance/rules/{batch_id}", response_model=List[RuleInfo], tags=["Provenance"])
def get_rules(batch_id: str):
    q = """
        SELECT rule_id, description, created_at
        FROM provenance_rules_applied
        WHERE batch_id = %s
        ORDER BY created_at ASC
    """
    rows = fetch_all(q, (batch_id,))
    return [
        RuleInfo(rule_id=r[0], description=r[1], applied_time=str(r[2]))
        for r in rows
    ]


# 4. Get latest batches for a source
@app.get("/provenance/source/{source_name}", tags=["Provenance"])
def get_batches_for_source(source_name: str, limit: int = 20):
    q = """
        SELECT batch_id, status, ingest_time
        FROM provenance_batch
        WHERE source_name = %s
        ORDER BY ingest_time DESC
        LIMIT %s
    """
    rows = fetch_all(q, (source_name, limit))
    return [{"batch_id": r[0], "status": r[1], "ingest_time": str(r[2])} for r in rows]


# 5. Search across all batches (optional)
@app.get("/provenance/search", tags=["Provenance"])
def search_batches(status: Optional[str] = None, source: Optional[str] = None):
    base = "SELECT batch_id, source_name, status, ingest_time FROM provenance_batch WHERE TRUE"
    params = []

    if status:
        base += " AND status = %s"
        params.append(status)

    if source:
        base += " AND source_name = %s"
        params.append(source)

    base += " ORDER BY ingest_time DESC"

    rows = fetch_all(base, params)
    return [{"batch_id": r[0], "source_name": r[1], "status": r[2], "ingest_time": str(r[3])} for r in rows]
