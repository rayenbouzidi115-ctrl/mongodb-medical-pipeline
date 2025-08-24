import os, time, glob, hashlib, re, csv
from datetime import datetime
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "healthcare")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "patients")
FILE_PATTERN = os.getenv("FILE_PATTERN", "healthcare_dataset-*.csv")
DATA_DIR = "/data"
SCAN_INTERVAL_SECS = int(os.getenv("SCAN_INTERVAL_SECS", "10"))

def log(*args):
    print("[ETL]", *args, flush=True)

def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_medications(meds_str):
    if pd.isna(meds_str) or str(meds_str).strip() == "":
        return []
    parts = re.split(r"[|,;]", str(meds_str))
    meds = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        m = re.match(r"^\s*([A-Za-z][\w\s\-\/]+?)(?:\s+(\d+(?:\.\d+)?\s*(?:mg|mcg|g|ml)))?\s*$", p)
        if m:
            name = m.group(1).strip()
            dosage = m.group(2).strip() if m.group(2) else None
        else:
            name, dosage = p, None
        meds.append({"name": name, "dosage": dosage})
    return meds

def to_int_safe(v):
    try:
        if pd.isna(v) or v == "":
            return None
        return int(float(v))
    except Exception:
        return None

def _split_name(fullname):
    if not fullname or pd.isna(fullname):
        return None, None
    parts = str(fullname).strip().split()
    if len(parts) == 1:
        return parts[0].title(), None
    return " ".join(parts[:-1]).title(), parts[-1].title()

def normalize_record(row, source_file):
    first = row.get("FirstName") or row.get("first_name") or row.get("First Name") or row.get("firstname")
    last  = row.get("LastName")  or row.get("last_name")  or row.get("Last Name")  or row.get("lastname")
    if (not first and not last) and row.get("Name"):
        first, last = _split_name(row.get("Name"))

    pid = row.get("PatientID") or row.get("patient_id") or row.get("id") or row.get("ID")
    gender = row.get("Gender") or row.get("sex")
    dob_raw = row.get("DateOfBirth") or row.get("dob") or row.get("Date of Birth")
    age_raw = row.get("Age") or row.get("age")
    doa_raw = row.get("DateOfAdmission") or row.get("admission_date") or row.get("Date of Admission")
    hospital = row.get("Hospital") or row.get("hospital")
    doctor = row.get("Doctor") or row.get("doctor")
    condition = row.get("MedicalCondition") or row.get("Medical Condition") or row.get("Condition") or row.get("medical_condition")
    meds = row.get("Medications") or row.get("Medication") or row.get("medications")
    allergies = row.get("Allergies") or row.get("allergies")
    city = row.get("Address_City") or row.get("City")
    state = row.get("Address_State") or row.get("State")
    zipc = row.get("Address_Zip") or row.get("Zip") or row.get("PostalCode")
    country = row.get("Country") or row.get("country")

    def parse_date(x):
        if pd.isna(x) or str(x).strip() == "":
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(str(x), fmt)
            except Exception:
                continue
        try:
            return pd.to_datetime(x, errors="coerce").to_pydatetime()
        except Exception:
            return None

    dob = parse_date(dob_raw)
    doa = parse_date(doa_raw)

    doc = {
        "patient_id": str(pid).strip() if pid not in [None, ""] else None,
        "name": {
            "first": str(first).strip().title() if first not in [None, ""] else None,
            "last":  str(last).strip().title()  if last  not in [None, ""] else None,
        },
        "gender": (str(gender).strip().title() if gender not in [None, ""] else None),
        "dob": dob,
        "age": to_int_safe(age_raw),
        "admission": {
            "date": doa,
            "hospital": str(hospital).strip().title() if hospital not in [None, ""] else None,
            "doctor":  str(doctor).strip().title()  if doctor  not in [None, ""] else None,
        },
        "medical_condition": (str(condition).strip().title() if condition not in [None, ""] else None),
        "medications": parse_medications(meds),
        "allergies": [a.strip().title() for a in re.split(r"[|,;]", str(allergies))]
                     if allergies not in [None, ""] and not pd.isna(allergies) else [],
        "address": {
            "city": str(city).strip().title() if city not in [None, ""] else None,
            "state": str(state).strip().upper() if state not in [None, ""] else None,
            "zip": str(zipc).strip() if zipc not in [None, ""] else None,
            "country": str(country).strip().title() if country not in [None, ""] else None,
        },
        "source_file": os.path.basename(source_file),
        "ingested_at": datetime.utcnow(),
    }

    def prune(obj):
        if isinstance(obj, dict):
            return {k: prune(v) for k, v in obj.items() if v not in [None, [], {}]}
        if isinstance(obj, list):
            return [prune(x) for x in obj if x not in [None, [], {}]]
        return obj

    return prune(doc)

def ensure_indexes(col):
    col.create_index("patient_id")
    col.create_index([("admission.date", 1)])
    col.create_index([("medical_condition", 1)])
    col.create_index([("name.first", 1)])
    col.create_index([("medications.name", 1)])

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]
    logs = db["ingestion_logs"]

    ensure_indexes(col)
    log("Connected. Watching", os.path.join(DATA_DIR, FILE_PATTERN))

    while True:
        try:
            files = sorted(glob.glob(os.path.join(DATA_DIR, FILE_PATTERN)))
            for fpath in files:
                sha = file_sha256(fpath)
                if logs.find_one({"file": os.path.basename(fpath), "sha256": sha}):
                    continue

                log("Processing:", fpath)

                # Auto-détection du séparateur (fallback ';')
                sep = ";"
                try:
                    with open(fpath, "r", encoding="utf-8") as fh:
                        sample = fh.read(2048)
                        fh.seek(0)
                        dialect = csv.Sniffer().sniff(sample)
                        sep = dialect.delimiter or ";"
                except Exception:
                    pass

                df = pd.read_csv(fpath, sep=sep, engine="python")
                df = df.where(pd.notnull(df), None)

                docs = []
                for _, row in df.iterrows():
                    doc = normalize_record(row.to_dict(), source_file=fpath)
                    docs.append(doc)

                if docs:
                    ops = [
                        UpdateOne(
                            {"patient_id": d.get("patient_id"),
                             "admission.date": d.get("admission", {}).get("date")},
                            {"$set": d},
                            upsert=True
                        )
                        for d in docs
                    ]
                    res = col.bulk_write(ops, ordered=False)
                    log(f"Upserted {res.upserted_count} / Modified {res.modified_count}")

                logs.insert_one({
                    "file": os.path.basename(fpath),
                    "sha256": sha,
                    "rows": len(docs),
                    "ts": datetime.utcnow()
                })
                log("Done:", fpath)

        except PyMongoError as e:
            log("Mongo error:", e)
        except Exception as e:
            log("Error:", e)

        time.sleep(SCAN_INTERVAL_SECS)

if __name__ == "__main__":
    main()
