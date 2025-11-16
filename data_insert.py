# Generating a high-volume synthetic clinical data package (CSV, JSONL, HL7) locally
# This code will:
# - Create a folder /mnt/data/data_source with subfolders hospital_a, clinic_b, hospital_c_hl7
# - Generate multiple files with realistic-ish synthetic clinical records containing PHI
# - Create a ZIP file at /mnt/data/clinical_data_sources.zip for download
# - Print summary of files created
#
# NOTE: This runs in the notebook environment and the resulting ZIP will be available for download.

import os
import random
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
import zipfile

random.seed(42)

OUT_DIR = Path("D:\DE_Assignment\data_source")
HOSP_A_DIR = OUT_DIR / "hospital_a"
CLINIC_B_DIR = OUT_DIR / "clinic_b"
HOSP_C_DIR = OUT_DIR / "hospital_c_hl7"
ZIP_PATH = Path("D:\DE_Assignment\clinical_data_sources.zip")

# Ensure directories exist
for d in [HOSP_A_DIR, CLINIC_B_DIR, HOSP_C_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Small name lists for synthetic diversity
first_names = ["John","Maria","Carlos","Linda","Adam","Jane","Rajesh","Tom","Emily","Robert","Susan","Michael","Anna","David","Priya","Kiran","Olivia","Liam","Noah","Sophia"]
last_names = ["Doe","Williams","D'Souza","Gonzalez","Smith","Miller","Kumar","Harris","Clark","King","Patel","Sharma","Johnson","Brown","Davis","Garcia","Martinez","Lopez","Wilson","Anderson"]
diagnoses = ["Hypertension","Flu","Diabetes","Asthma","Migraine","Back Pain","Cough","Chest Pain","Allergy","Anemia","Depression","Anxiety","Fracture","COPD","Pneumonia"]
addresses = ["221B Baker Street","10 Downing St","12 King St","25 River Road","101 Main St","500 Market Ave","13 Elm St","742 Evergreen Terrace","1600 Pennsylvania Ave","4 Privet Drive"]
email_domains = ["example.com","healthmail.com","clinic.org","medmail.net"]
phones = [f"555-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(200)]

# Utility functions
def rand_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def rand_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"

def rand_dob(start_year=1940, end_year=2005):
    year = random.randint(start_year, end_year)
    month = random.randint(1,12)
    day = random.randint(1,28)
    return f"{year:04d}-{month:02d}-{day:02d}"

def rand_email(name):
    local = name.lower().replace(" ", ".").replace("'", "")
    return f"{local}@{random.choice(email_domains)}"

def rand_address():
    return random.choice(addresses)

def rand_note(name):
    extras = [
        f"Contact: {random.choice(phones)}",
        f"Address: {rand_address()}",
        f"Email: {rand_email(name)}",
        "Follow-up scheduled.",
        "Patient reports severe symptoms.",
        "No known allergies.",
        "Family history of heart disease."
    ]
    return f"{random.choice(extras)}"

# Parameters for "high volume"
# Hospital A: 20 CSV files, each 5,000 rows -> 100,000 rows
# Clinic B: 20 JSONL files, each 5,000 records -> 100,000 records
# HL7: 10,000 messages split across 50 files -> 10,000 messages
num_csv_files = 20
rows_per_csv = 5000

num_jsonl_files = 20
records_per_jsonl = 5000

total_hl7_messages = 10000
hl7_files = 50
msgs_per_hl7 = total_hl7_messages // hl7_files

start_date = datetime(2025, 2, 1)

# Generate Hospital A CSV files
csv_files_created = []
for i in range(num_csv_files):
    file_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    fname = HOSP_A_DIR / f"{file_date}_clinical.csv"
    csv_files_created.append(fname.name)
    with open(fname, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["patient_id","patient_name","ssn","dob","visit_date","diagnosis","notes"])
        for r in range(rows_per_csv):
            pid = f"P{10000 + i*rows_per_csv + r}"
            name = rand_name()
            ssn = rand_ssn()
            dob = rand_dob()
            visit_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            diag = random.choice(diagnoses)
            note = f"Patient {name} attended for {diag}. {rand_note(name)}"
            writer.writerow([pid, name, ssn, dob, visit_date, diag, note])

# Generate Clinic B JSONL files
jsonl_files_created = []
for i in range(num_jsonl_files):
    file_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    fname = CLINIC_B_DIR / f"clinical_{file_date}.jsonl"
    jsonl_files_created.append(fname.name)
    with open(fname, "w", encoding='utf-8') as f:
        for r in range(records_per_jsonl):
            rid = f"C{20000 + i*records_per_jsonl + r}"
            name = rand_name()
            dob = rand_dob()
            encounter = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            icd = random.choice(["J10","E11","M54","I10","G43","K21"])
            free_text = f"{random.choice(['Complained of cough.', 'Diagnosed with condition.', 'Follow-up needed.', 'No acute complaints.'])} {rand_note(name)}"
            rec = {
                "id": rid,
                "name": name,
                "date_of_birth": dob,
                "encounter": encounter,
                "icd": icd,
                "free_text": free_text
            }
            f.write(json.dumps(rec) + "\n")

# Generate HL7 files
hl7_files_created = []
pid_counter = 30000
for i in range(hl7_files):
    fname = HOSP_C_DIR / f"msgs_{i+1:03d}.hl7"
    hl7_files_created.append(fname.name)
    with open(fname, "w", encoding='utf-8') as f:
        for m in range(msgs_per_hl7):
            pid_counter += 1
            msg_id = f"MSG{pid_counter}"
            name = rand_name()
            dob = rand_dob()
            dob_hl7 = dob.replace("-", "")
            phone = random.choice(phones)
            address = rand_address()
            ssn = rand_ssn()
            msh_ts = (start_date + timedelta(days=(i % 30), seconds=m)).strftime("%Y%m%d%H%M%S")
            # Simple HL7 v2-ish message
            msh = f"MSH|^~\\&|HOSPITAL_C|LAB|QLM_SYS|DEST|{msh_ts}||ORU^R01|{msg_id}|P|2.3"
            pid = f'PID|1||{pid_counter}||"{name}"||{dob_hl7}|M|||{address}|||||{ssn}'
            obr = f"OBR|1||{1000 + m}|TEST^TESTNAME"
            obx = f"OBX|1|ST|RESULT||{random.randint(1,200)}|units||N"
            f.write(msh + "\n" + pid + "\n" + obr + "\n" + obx + "\n\n")

# Create provenance JSON folder copies (one summary per file)
prov_dir = OUT_DIR / "provenance"
prov_dir.mkdir(exist_ok=True)
prov_summaries = []
# For CSVs
for fname in csv_files_created:
    batch_id = fname.replace(".csv","")
    p = {
        "batch_id": batch_id,
        "source": "hospital_a",
        "input_file": str(HOSP_A_DIR / fname),
        "ingest_time": datetime.utcnow().isoformat()+"Z",
        "raw_rows": rows_per_csv
    }
    with open(prov_dir / f"{batch_id}.json", "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2)
    prov_summaries.append(p)
# For JSONLs
for fname in jsonl_files_created:
    batch_id = fname.replace(".jsonl","")
    p = {
        "batch_id": batch_id,
        "source": "clinic_b",
        "input_file": str(CLINIC_B_DIR / fname),
        "ingest_time": datetime.utcnow().isoformat()+"Z",
        "raw_rows": records_per_jsonl
    }
    with open(prov_dir / f"{batch_id}.json", "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2)
    prov_summaries.append(p)
# For HL7
for fname in hl7_files_created:
    batch_id = fname.replace(".hl7","")
    p = {
        "batch_id": batch_id,
        "source": "hospital_c_hl7",
        "input_file": str(HOSP_C_DIR / fname),
        "ingest_time": datetime.utcnow().isoformat()+"Z",
        "raw_messages": msgs_per_hl7
    }
    with open(prov_dir / f"{batch_id}.json", "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2)
    prov_summaries.append(p)

# Zip the entire data_source folder
with zipfile.ZipFile(ZIP_PATH, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    for folder_name, subfolders, filenames in os.walk(OUT_DIR):
        for filename in filenames:
            file_path = os.path.join(folder_name, filename)
            arcname = os.path.relpath(file_path, "/mnt/data")
            zipf.write(file_path, arcname=arcname)

# Summary
summary = {
    "hospital_a_csv_files": len(csv_files_created),
    "rows_per_csv": rows_per_csv,
    "clinic_b_jsonl_files": len(jsonl_files_created),
    "records_per_jsonl": records_per_jsonl,
    "hl7_files": len(hl7_files_created),
    "messages_per_hl7_file": msgs_per_hl7,
    "zip_path": str(ZIP_PATH),
    "generated_at": datetime.utcnow().isoformat() + "Z"
}

print("Generation complete. Summary:")
print(json.dumps(summary, indent=2))

# Provide a simple sample of first lines for quick preview (one file each)
def head(path, n=5):
    lines = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for _ in range(n):
                line = f.readline()
                if not line:
                    break
                lines.append(line.rstrip("\n"))
    except Exception as e:
        lines = [f"Error reading: {e}"]
    return lines

preview = {
    "hospital_a_sample": head(HOSP_A_DIR / csv_files_created[0], 3),
    "clinic_b_sample": head(CLINIC_B_DIR / jsonl_files_created[0], 3),
    "hl7_sample": head(HOSP_C_DIR / hl7_files_created[0], 12)
}

print("\nPreview (first lines of sample files):")
print(json.dumps(preview, indent=2))

# Expose path for download via notebook UI
print(f"\n[Download the data package](/mnt/data/clinical_data_sources.zip)")

