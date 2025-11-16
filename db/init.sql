-- Active: 1763198906108@@127.0.0.1@5432@assignment

CREATE TABLE IF NOT EXISTS provenance_batch (
    batch_id            VARCHAR(150) PRIMARY KEY,
    source_name         VARCHAR(100) NOT NULL,
    ingest_time         TIMESTAMP NOT NULL DEFAULT NOW(),
    raw_file_path       TEXT NOT NULL,
    raw_sha256          TEXT NOT NULL,
    curated_sha256      TEXT,
    final_sha256        TEXT,
    status              VARCHAR(50) NOT NULL DEFAULT 'INGESTED', -- INGESTED, VALIDATED, SCRUBBED, TRANSFORMED, COMPLETED, FAILED
    version_path        TEXT,
    total_rows          INT,
    error_details       TEXT,
    rule_set_version    VARCHAR(50),
    created_at          TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS provenance_steps (
    id                  SERIAL PRIMARY KEY,
    batch_id            VARCHAR(150) REFERENCES provenance_batch(batch_id),
    step_name           VARCHAR(100) NOT NULL,    -- ingest, validate, scrub_phi, transform, write_version
    step_time           TIMESTAMP NOT NULL DEFAULT NOW(),
    details_json        JSONB,
    created_at          TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS provenance_rules_applied (
    id                  SERIAL PRIMARY KEY,
    batch_id            VARCHAR(150) REFERENCES provenance_batch(batch_id),
    rule_id             VARCHAR(50) NOT NULL,
    description         TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_registry (
    source_name     VARCHAR(100) PRIMARY KEY,
    source_type     VARCHAR(50) NOT NULL,    -- csv, jsonl, hl7
    file_path       TEXT NOT NULL,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control_header (
    control_id      SERIAL PRIMARY KEY,
    source_name     VARCHAR(100) REFERENCES source_registry(source_name),
    status          VARCHAR(50) DEFAULT 'PENDING',   -- PENDING, RUNNING, COMPLETED, FAILED
    scheduled_time  TIMESTAMP DEFAULT NOW(),
    run_time        TIMESTAMP,
    comments        TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control_detail (
    id              SERIAL PRIMARY KEY,
    control_id      INT REFERENCES control_header(control_id),
    step_order      INT NOT NULL,                 -- 1,2,3...
    step_type       VARCHAR(50) NOT NULL,         -- ingest, validate, scrub, transform, version, etc.
    config_json     JSONB,                        -- dynamic config
    created_at      TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS schema_metadata (
    id              SERIAL PRIMARY KEY,
    source_name     VARCHAR(100) REFERENCES source_registry(source_name),
    column_name     VARCHAR(150),
    data_type       VARCHAR(100),
    is_nullable     BOOLEAN,
    is_phi          BOOLEAN DEFAULT FALSE,
    phi_rule        VARCHAR(50),                  -- if applicable
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS phi_rules (
    rule_id         VARCHAR(50) PRIMARY KEY,
    pattern         TEXT NOT NULL,
    replacement     TEXT NOT NULL,
    description     TEXT,
    rule_version    VARCHAR(50) DEFAULT 'v1',
    created_at      TIMESTAMP DEFAULT NOW()
);


CREATE TABLE audit_log (
  id serial PRIMARY KEY,
  actor text,
  action text,
  batch_id text,
  details jsonb,
  severity text,
  created_at timestamptz DEFAULT now()
);




INSERT INTO source_registry (source_name, source_type, file_path, active)
VALUES
('hospital_a', 'csv', 'data_source/hospital_a', TRUE),
('clinic_b', 'jsonl', 'data_source/clinic_b', TRUE),
('hospital_c_hl7', 'hl7', 'data_source/hospital_c_hl7', TRUE);


INSERT INTO schema_metadata 
(source_name, column_name, data_type, is_nullable, is_phi)
VALUES
('hospital_a','patient_id','string', FALSE, FALSE),
('hospital_a','patient_name','string', FALSE, TRUE),
('hospital_a','ssn','string', FALSE, TRUE),
('hospital_a','dob','date', TRUE, TRUE),
('hospital_a','visit_date','date', TRUE, FALSE),
('hospital_a','diagnosis','string', TRUE, FALSE),
('hospital_a','notes','string', TRUE, FALSE);


INSERT INTO schema_metadata 
(source_name, column_name, data_type, is_nullable, is_phi)
VALUES
('clinic_b','id','string', FALSE, FALSE),
('clinic_b','name','string', FALSE, TRUE),
('clinic_b','date_of_birth','date', TRUE, TRUE),
('clinic_b','encounter','date', TRUE, FALSE),
('clinic_b','icd','string', TRUE, FALSE),
('clinic_b','free_text','string', TRUE, TRUE);


INSERT INTO schema_metadata 
(source_name, column_name, data_type, is_nullable, is_phi)
VALUES
('hospital_c_hl7','PID-3','string', FALSE, FALSE),
('hospital_c_hl7','PID-5','string', FALSE, TRUE),
('hospital_c_hl7','PID-7','date', FALSE, TRUE),
('hospital_c_hl7','PID-11','string', TRUE, TRUE),
('hospital_c_hl7','PID-17','string', TRUE, TRUE);


SELECT * FROM schema_metadata ORDER BY source_name, id;

--UPDATE provenance_batch
--SET status = 'INGESTED',
   -- error_details = NULL
--WHERE status = 'FAILED_VALIDATION';


--DELETE FROM provenance_batch;
--DELETE FROM provenance_steps;
--DELETE FROM provenance_rules_applied;

--DELETE FROM audit_log;
--DELETE FROM schema_metadata where source_name = "hospital_c_hl7";

--UPDATE provenance_batch
--SET status = 'SCRUBBED'


-- PHI rule set v1 (example)
INSERT INTO phi_rules (rule_id, pattern, replacement, description, rule_version)
VALUES
('PHI_SSN', '\\b\\d{3}-\\d{2}-\\d{4}\\b', '[REDACTED_SSN]', 'US SSN pattern xxx-xx-xxxx', 'v1'),
('PHI_SSN_DIGITS', '\\b\\d{9}\\b', '[REDACTED_SSN]', '9-digit SSN', 'v1'),
('PHI_PHONE', '\\b\\d{3}-\\d{3}-\\d{4}\\b', '[REDACTED_PHONE]', 'Phone like 555-123-4567', 'v1'),
('PHI_EMAIL', '\\b[\\w.+-]+@[\\w-]+\\.[\\w.-]+\\b', '[REDACTED_EMAIL]', 'Email addresses', 'v1'),
('PHI_DATE_YYYYMMDD', '\\b\\d{8}\\b', '[REDACTED_DATE]', 'Date like 20250215', 'v1'),
('PHI_DATE_ISO', '\\b\\d{4}-\\d{2}-\\d{2}\\b', '[REDACTED_DATE]', 'Date like 2025-02-15', 'v1'),
('PHI_ADDRESS_SIMPLE', '\\b\\d+\\s+[A-Za-z0-9\\.\\- ]+\\b', '[REDACTED_ADDRESS]', 'Simple street address (approx)', 'v1');


--SELECT batch_id, status FROM provenance_batch ORDER BY ingest_time DESC;




--UPDATE provenance_batch SET status='VALIDATED', curated_sha256=NULL, final_sha256=NULL, version_path=NULL, error_details=NULL;


-- infra/create_pg_roles.sql
-- Run as postgres superuser

CREATE ROLE qlm_admin LOGIN PASSWORD 'qlm_admin_pass';
CREATE ROLE qlm_etl LOGIN PASSWORD 'qlm_etl_pass';
CREATE ROLE qlm_analyst LOGIN PASSWORD 'qlm_analyst_pass';
CREATE ROLE qlm_audit LOGIN PASSWORD 'qlm_audit_pass';


-- ETL needs read/write on provenance tables
GRANT CONNECT ON DATABASE ASSIGNMENT TO qlm_etl, qlm_analyst, qlm_audit;
GRANT USAGE ON SCHEMA public TO qlm_etl, qlm_analyst, qlm_audit;

-- grant rights (adjust table names if different)
GRANT SELECT, INSERT, UPDATE ON provenance_batch, provenance_steps, provenance_rules_applied TO qlm_etl;
GRANT SELECT ON provenance_batch, provenance_steps, provenance_rules_applied TO qlm_analyst;
GRANT SELECT ON provenance_batch, provenance_steps, provenance_rules_applied TO qlm_audit;

-- Admin full rights
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO qlm_admin;


CREATE TABLE dataset_versions (
  id serial PRIMARY KEY,
  source_name text,
  version_ts timestamptz,
  path text,
  sha256 text,
  created_by text
);
