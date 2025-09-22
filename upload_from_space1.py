import os
import csv
import io
import time
import psutil
from threading import Thread, Lock
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
import boto3
from flask import Flask, jsonify

# -----------------------------
# Load env
# -----------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MODE = os.getenv("COLLECTION_MODE", "per_file")
FIXED_COLLECTION = os.getenv("COLLECTION_NAME")

SPACES_KEY = os.getenv("SPACES_KEY")
SPACES_SECRET = os.getenv("SPACES_SECRET")
SPACES_REGION = os.getenv("SPACES_REGION")
SPACES_BUCKET = os.getenv("SPACES_BUCKET")
SPACES_PREFIX = os.getenv("SPACES_PREFIX", "")

# -----------------------------
# Mongo + Spaces
# -----------------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

s3 = boto3.client(
    "s3",
    region_name=SPACES_REGION,
    endpoint_url=f"https://{SPACES_REGION}.digitaloceanspaces.com",
    aws_access_key_id=SPACES_KEY,
    aws_secret_access_key=SPACES_SECRET
)

# -----------------------------
# Global State
# -----------------------------
inserted_count = 0
total_count = 0
job_running = False
lock = Lock()
start_time = None

# -----------------------------
# Insert CSV Stream
# -----------------------------
def insert_csv_from_stream(stream, collection_name):
    global inserted_count, total_count
    collection = db[collection_name]
    reader = csv.reader(io.StringIO(stream.read().decode("utf-8")))
    rows = list(reader)
    total_count += len(rows)

    for row in tqdm(rows, desc=f"Inserting: {collection_name}", unit="row"):
        if len(row) < 2:
            continue
        doc = {"url_id": row[0], "input_url": row[1], "status": "pending"}
        try:
            collection.insert_one(doc)
            with lock:
                inserted_count += 1
        except Exception as e:
            print(f"❌ Error inserting row: {e}")

# -----------------------------
# ETL Process
# -----------------------------
def process_all_csv_from_spaces():
    global job_running, start_time
    job_running = True
    start_time = time.time()

    print(f"📦 Scanning bucket `{SPACES_BUCKET}` under prefix `{SPACES_PREFIX}`...\n")
    objects = s3.list_objects_v2(Bucket=SPACES_BUCKET, Prefix=SPACES_PREFIX)

    if 'Contents' not in objects:
        print("⚠️ No files found.")
        job_running = False
        return

    csv_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".csv")]
    print(f"📂 Found {len(csv_files)} CSV files\n")

    for key in csv_files:
        filename = os.path.basename(key)
        collection_name = os.path.splitext(filename)[0] if MODE == "per_file" else FIXED_COLLECTION

        print(f"📤 Downloading `{filename}` from Spaces → inserting into `{collection_name}`")
        response = s3.get_object(Bucket=SPACES_BUCKET, Key=key)
        insert_csv_from_stream(response['Body'], collection_name)

    job_running = False
    print("\n🎯 All uploads complete.")

# -----------------------------
# Flask API for control + stats
# -----------------------------
app = Flask(__name__)

@app.route("/start")
def start_job():
    global job_running
    if job_running:
        return {"status": "⚠️ Job already running"}
    t = Thread(target=process_all_csv_from_spaces)
    t.start()
    return {"status": "🚀 Job started"}

@app.route("/stop")
def stop_job():
    global job_running
    job_running = False
    return {"status": "🛑 Stop signal sent (will finish current file)"}

@app.route("/stats")
def stats():
    cpu = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    uptime = int(time.time() - start_time) if start_time else 0
    return jsonify({
        "inserted": inserted_count,
        "total": total_count,
        "progress": f"{(inserted_count/total_count*100):.2f}%" if total_count else "0%",
        "cpu_usage": f"{cpu}%",
        "memory_usage": f"{mem.percent}%",
        "uptime_seconds": uptime,
        "job_running": job_running,
        "healthy": cpu < 80 and mem.percent < 80
    })

# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
