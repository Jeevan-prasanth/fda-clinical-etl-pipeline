

# 📘 FDA-Grade Clinical ETL Pipeline + Regulated Data Lake (QLM)

**Author:** *Jeevanprasanth T*  
**Date:** *16/11/2025*

---

# 🚀 Overview

This project implements an **FDA/HIPAA-compliant clinical ETL pipeline** and a **tiered, regulated data lake** designed for **Quality Language Model (QLM)** workflows.  
It supports:

- Full lineage + provenance  
- Deterministic reproducible processing  
- PHI scrubbing  
- Canonical data transformation  
- Integrity hashing (SHA-256)  
- Audit logging  
- Dataset versioning (Apache Hudi)  
- Regulatory alignment (HIPAA, FDA, CFR Part 11)  

---

# 📂 Project Structure

```

DE_Assignment/
│
├── data/
│   ├── raw/
│   ├── curated/
│   ├── qlm_ready/
│   └── hudi/
│
├── etl/
│   ├── ingest.py
│   ├── validate.py
│   ├── scrub.py
│   ├── transform.py
│   ├── provenance_recorder.py
│   ├── db.py
│   └── audit.py
│
├── api/
│   └── provenance_api.py
│
├── hudi/
│   ├── spark_session.py
│   └── register_hudi.py
│
├── test/
│   ├── run_ingest.py
│   ├── run_validate.py
│   ├── run_scrub.py
│   └── run_transform.py
│
├── sql/
│   └── schema.sql
│
└── README.md

````

---

# 🎯 TASK 1 — FDA-Grade ETL Pipeline

## ✔ Capabilities

| Feature | Status |
|--------|--------|
| Multi-source ingestion (CSV, JSONL, HL7) | ✔ |
| Schema validation | ✔ |
| PHI scrubbing | ✔ |
| Canonical QLM transformation | ✔ |
| Provenance tracking (batch + steps + rules) | ✔ |
| Integrity verification (SHA-256) | ✔ |
| Audit logging | ✔ |
| REST API for lineage | ✔ |

---

# 🧬 ETL Flow Diagram

```mermaid
flowchart TD

A[Raw Clinical Data] --> B[INGEST]
B --> C[VALIDATE]
C -->|Passed| D[PHI SCRUB]
C -->|Failed| ERR[FAILED_VALIDATION]

D --> E[TRANSFORM → Canonical QLM Schema]
E --> F[data/qlm_ready]

B --> PB[(provenance_batch)]
C --> PS[(provenance_steps)]
D --> PS
E --> PS

PS --> API[/Provenance API/]
API --> AUD[(audit_log)]
````

---

# 🔐 PHI Scrubbing Rules

* Names → Redacted
* Addresses → Removed
* SSN → Removed
* DOB → Masked / shifted
* Free-text PHI → Regex scrubbing
* HL7 PID segments normalized

---

# 🔎 Provenance API (FastAPI)

| Endpoint                                 | Purpose               |
| ---------------------------------------- | --------------------- |
| `GET /provenance/batch/{batch_id}`       | Full batch lineage    |
| `GET /provenance/steps/{batch_id}`       | Step-by-step timeline |
| `GET /provenance/rules/{batch_id}`       | PHI rules applied     |
| `GET /provenance/search?source=&status=` | Query by metadata     |

📄 **OpenAPI Docs:**
[http://localhost:8080/docs](http://localhost:8080/docs)

---

# 🎯 TASK 2 — Tiered Regulated Data Lake

## ✔ Zones Implemented

* **RAW** → Contains PHI
* **CURATED** → Redacted, PHI-free
* **QLM_READY** → Canonical ML Parquet
* **HUDI** → Versioned dataset (ACID, time-travel)

---

# 🧱 Logical Architecture

```mermaid
flowchart TD

R[data/raw] --> C[data/curated] --> Q[data/qlm_ready] --> H[data/hudi]

R --> PB[(provenance_batch)]
C --> PS[(provenance_steps)]
Q --> PS
H --> VER[(dataset_versions)]

PS --> API[/Lineage API/]
API --> AUD[(audit_log)]
```

---

# 🛡️ RBAC — Access Control Matrix

| Resource     | admin | compliance | engineer | scientist | api_user |
| ------------ | ----- | ---------- | -------- | --------- | -------- |
| Raw Zone     | R/W   | R          | -        | -         | -        |
| Curated      | R/W   | R          | R        | -         | -        |
| QLM-ready    | R/W   | R          | R        | R         | -        |
| Hudi Layer   | R/W   | R          | R        | R         | -        |
| provenance_* | R/W   | R          | R        | R         | R        |
| audit_log    | R/W   | R          | -        | -         | -        |

---

# 🧪 Versioned Dataset (Hudi)

### ✨ Capabilities Enabled:

* ACID transactions
* Incremental data ingestion
* Commit history
* Time travel
* Snapshot queries

### Snapshot Read

```python
df = spark.read.format("hudi").load("data/hudi/hospital_a")
```

### Time Travel

```python
df_old = spark.read.format("hudi")\
    .option("as.of.instant", "20251116012100")\
    .load("data/hudi/hospital_a")
```

---

# 📊 Monitoring & Audit

### Logged Automatically:

* Every ETL stage
* Every file hash
* Every PHI rule applied
* Errors
* User actions
* Timestamps

### Queries

**Failed batches:**

```sql
SELECT * FROM provenance_batch WHERE status LIKE 'FAILED_%';
```

**Latest audit entries:**

```sql
SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 50;
```

---

# 🏛️ FDA / HIPAA / CFR Part 11 Compliance Mapping

## ✔ HIPAA

| Requirement           | Implementation                     |
| --------------------- | ---------------------------------- |
| PHI Minimization      | PHI removed in curated + qlm zones |
| Access Control        | RBAC                               |
| Integrity             | SHA-256                            |
| Audit Controls        | audit_log                          |
| Transmission Security | TLS-ready                          |

---

## ✔ FDA QLM Workflow

| Requirement        | Implementation     |
| ------------------ | ------------------ |
| Traceability       | provenance + audit |
| Reproducibility    | QLM-ready + Hudi   |
| Data Integrity     | hashing            |
| Controlled Process | deterministic ETL  |

---

## ✔ CFR Part 11

| Requirement        | Implementation       |
| ------------------ | -------------------- |
| Audit Trail        | immutable logs       |
| Electronic Records | Parquet + versioning |
| Role Access        | RBAC                 |
| Timestamp accuracy | UTC timestamps       |

---

# 🛠️ Deployment Instructions

### 1️⃣ Install Dependencies

```
pip install -r requirements.txt
```

### 2️⃣ Setup Database

```
psql -f db/init.sql
```

### 3️⃣ Run ETL

```
python -m test.run_ingest
python -m test.run_validate
python -m test.run_scrub
python -m test.run_transform
```

### 4️⃣ Run Provenance API

```
uvicorn api.provenance_api:app --reload --port 8080
```

### 5️⃣ (Optional) Run Hudi Versioning

```
python -m hudi.register_hudi hospital_a
```

---

# 🎉 End of README.md

This README contains all required documentation for **Task 1 + Task 2**.

```


