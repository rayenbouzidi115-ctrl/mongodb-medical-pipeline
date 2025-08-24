# 🏥 Medical ETL Pipeline — MongoDB + Docker

## 🎯 Objective
This project implements a mini **ETL → MongoDB** pipeline using Docker.  
I ingest a medical CSV dataset, normalize the records, and answer a series of **queries** (filters & aggregations) defined in the assignment.  
This README serves as my **report**.

---

## 🧱 Architecture (Docker)

- **mongo** — MongoDB 7 database  
- **mongo-express** — Web UI protected by Basic Auth  
- **app** — Python 3.11 service (ETL built with `pandas` + `pymongo`)

Project structure:
```
root/
├─ app/
│  ├─ etl.py
│  ├─ queries.py
│  └─ requirements.txt
├─ data/
│  └─ healthcare_dataset-*.csv
├─ reports/
│  └─ (generated) query_results.md
├─ docker-compose.yml
├─ .env (.env.example provided)
└─ README.md  ← this file
```

---

## ⚙️ Startup / Verification

```bash
# 1) Start the stack
docker compose up -d --build

# 2) Follow the ETL logs
docker compose logs -f app

# 3) Open Mongo-Express UI
#    Credentials = set in docker-compose (BasicAuth)
http://localhost:8081
```

In the logs I see:
`[ETL] Processing: /data/healthcare_dataset-....csv`  
then  
`Upserted ... / Modified ...`

---

## 🧩 Data Model (`patients` collection)

```json
{
  "patient_id": "P001",
  "name": { "first": "Thomas", "last": "Leroy" },
  "gender": "Male",
  "dob": "1965-09-12T00:00:00Z",
  "age": 59,
  "admission": {
    "date": "2024-02-14T00:00:00Z",
    "hospital": "Saint-Louis",
    "doctor": "Dr Martin"
  },
  "medical_condition": "Hypertension",
  "medications": [
    { "name": "Lipitor", "dosage": "20mg" },
    { "name": "Aspirin", "dosage": "100mg" }
  ],
  "allergies": ["Penicillin"],
  "address": { "city": "Paris", "state": "IDF", "zip": "75010", "country": "France" },
  "source_file": "healthcare_dataset-20250506.csv",
  "ingested_at": "YYYY-MM-DDTHH:MM:SSZ"
}
```

**Indexes automatically created**:  
`patient_id`, `admission.date`, `medical_condition`, `name.first`, `medications.name`.

**Schema rationale (3 points):**  
1. 1 document = 1 admission → direct queries, no joins.  
2. `medications` stored as array → allows `$unwind` + `$group` for frequency analysis.  
3. Indexes aligned with required filters → fast query response.

---

## 🧪 ETL (app/etl.py) — Main Features

- **Auto-detect CSV delimiter** (`;`, `,`, tab; fallback `;`).  
- Robust **date parsing** (`dob`, `admission.date`) with multiple formats.  
- Flexible column naming support (`Name`, `Medical Condition`, `Date of Admission`, etc.).  
- Normalization of **medications** into `{name, dosage}` objects (accepts `| , ;`).  
- **Idempotency**: SHA-256 file hash + **upsert** on `(patient_id, admission.date)`.  
- Ingestion logs stored in `ingestion_logs` collection.

---

## 🔍 Queries & Results (Mongo-Express)

> I use the **Advanced** tab (check *Aggregate query* only when grouping).  
> The count is shown at the top: “Documents 1–20 of **X**”.

### 1) Total patients
- **Observed result:** **1827** documents in `healthcare.patients`.  
- *Suggested screenshot:* `./img/01_total.png`

### 2) Admissions **after 2023-01-01**
```json
{ "$expr": { "$gte": [ [ { "$year": "$admission.date" } ], 2023 ] } }
```
- **Observed result:** **493** patients.  
- *Screenshot:* `./img/02_after_20230101.png`

### 3) Specific counts
**a) Age > 50**  
```json
{ "age": { "$gt": 50 } }
```
- **Result:** **925**  
- *Screenshot:* `./img/03_age_gt50.png`

**b) First name = “Thomas” (case-insensitive)**  
```json
{ "name.first": { "$regex": "^Thomas$", "$options": "i" } }
```
- **Result:** **11**  
- *Screenshot:* `./img/04_thomas.png`

**c) By medical condition**  
```json
[
  { "$match": { "medical_condition": { "$ne": null } } },
  { "$group": { "_id": "$medical_condition", "count": { "$sum": 1 } } },
  { "$sort": { "count": -1, "_id": 1 } }
]
```
- **Result (excerpt):** see screenshot `./img/06_by_condition.png`.

### 4) Medication frequency *(aggregation)*
```json
[
  { "$unwind": "$medications" },
  { "$group": { "_id": { "$toUpper": "$medications.name" }, "count": { "$sum": 1 } } },
  { "$sort": { "count": -1, "_id": 1 } }
]
```
- **Top (excerpt):** see screenshot `./img/07_med_frequency.png`.

### 5) Patients under “Lipitor”
```json
{ "medications.name": { "$regex": "^Lipitor$", "$options": "i" } }
```
- **Result:** **358**  
- *Screenshot:* `./img/05_lipitor.png`

---

## 🧾 Optional Auto-Report
```bash
docker compose run --rm app python queries.py
# -> reports/query_results.md
```
This generates a summary file with all query results.

---

## 🛠️ Data Quality / Troubleshooting Notes
- The real CSV uses `;` → handled by auto-detection.  
- Dates are heterogeneous → tolerant parsing in ETL.  
- If UI does not show **count**, I read the header line *Documents 1–20 of X*.  
- As fallback, I validate via shell:  
  ```js
  db.patients.countDocuments({})
  ```

---

## ✅ Conclusion
I successfully ingested **1827** admissions into `healthcare.patients`, and answered the required queries: post-2023 (**493**), patients >50 years old (**925**), first name “Thomas” (**11**), and patients under Lipitor (**358**).  
The pipeline is fully **dockerized, idempotent, and indexed**.
