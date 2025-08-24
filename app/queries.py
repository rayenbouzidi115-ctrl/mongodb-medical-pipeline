import os
from datetime import datetime
from pymongo import MongoClient
from pprint import pprint
from collections import defaultdict

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "healthcare")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "patients")
REPORT_PATH = os.getenv("REPORT_PATH", "/data/reports/query_results.md")

def run():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    lines = []
    def w(s=""):
        print(s)
        lines.append(s)

    w("# Query Results")
    w()
    # 1) How many patients in the collection?
    total = col.count_documents({})
    w(f"**1) Total patients (documents)**: {total}")
    w()

    # 2) Patients admitted after Jan 1, 2023
    cutoff = datetime(2023,1,1)
    cursor = col.find({"admission.date": {"$gt": cutoff}}, {"_id": 0, "name": 1, "admission.date": 1}).limit(50)
    results = list(cursor)
    w("**2) Patients admitted after 2023-01-01 (first 50):**")
    for r in results:
        nm = r.get("name", {})
        w(f"- {nm.get('first','?')} {nm.get('last','?')} — {r.get('admission',{}).get('date')}")
    w()

    # 3a) Count patients older than 50
    over50 = col.count_documents({"age": {"$gt": 50}})
    w(f"**3a) Patients older than 50**: {over50}")

    # 3b) Count with first name Thomas (case-insensitive)
    thomas = col.count_documents({"name.first": {"$regex": r'^Thomas$', "$options": "i"}})
    w(f"**3b) Patients with first name 'Thomas'**: {thomas}")

    # 3c) Count per distinct Medical Condition
    pipeline_cond = [
        {"$match": {"medical_condition": {"$ne": None}}},
        {"$group": {"_id": "$medical_condition", "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
    ]
    cond_counts = list(col.aggregate(pipeline_cond))
    w("**3c) Patients per Medical Condition:**")
    for c in cond_counts:
        w(f"- {c['_id']}: {c['count']}")
    w()

    # 4) Frequency of usage for each Medication
    pipeline_meds = [
        {"$unwind": "$medications"},
        {"$group": {"_id": {"$toUpper": "$medications.name"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
    ]
    med_counts = list(col.aggregate(pipeline_meds))
    w("**4) Medication usage frequency:**")
    for m in med_counts:
        w(f"- {m['_id']}: {m['count']}")
    w()

    # 5) All patients currently taking 'Lipitor'
    lipitor = list(col.find({"medications.name": {"$regex": r"^Lipitor$", "$options": "i"}},
                             {"_id": 0, "name": 1, "age": 1, "medical_condition": 1, "medications": 1}).limit(50))
    w("**5) Patients taking 'Lipitor' (first 50):**")
    for p in lipitor:
        nm = p.get("name", {})
        w(f"- {nm.get('first','?')} {nm.get('last','?')} — age {p.get('age','?')}, condition: {p.get('medical_condition','?')}")
    w()

    # Write report
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    run()