import os
import csv
import io
import time
import psutil
from threading import Thread, Lock
from pymongo import MongoClient
from dotenv import load_dotenv
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
# Download with Progress
# -----------------------------
def download_with_progress(bucket, key, filename):
    try:
        obj = s3.head_object(Bucket=bucket, Key=key)
        total_size = obj['ContentLength']
        print(f"\n⬇️ Downloading {filename} ({total_size/1024:.2f} KB)...")

        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']

        chunk_size = 1024 * 1024  # 1 MB
        downloaded = 0
        buffer = io.BytesIO()

        while True:
            data = body.read(chunk_size)
            if not data:
                break
            buffer.write(data)
            downloaded += len(data)
            percent = (downloaded / total_size) * 100
            print(f"   {downloaded/1024:.2f} KB / {total_size/1024:.2f} KB ({percent:.2f}%)", end="\r")

        print(f"\n✅ Finished downloading {filename}")
        buffer.seek(0)
        return buffer

    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        return None

# -----------------------------
# Insert CSV Stream (Improved)
# -----------------------------
def insert_csv_from_stream(stream, collection_name):
    global inserted_count, total_count
    collection = db[collection_name]

    try:
        reader = csv.reader(io.StringIO(stream.read().decode("utf-8")))
        rows = list(reader)
    except Exception as e:
        print(f"❌ Failed to read CSV stream for {collection_name}: {e}")
        return

    total_count += len(rows)
    print(f"📥 Starting insert for `{collection_name}` → {len(rows)} rows")

    for i, row in enumerate(rows, start=1):
        if len(row) < 2:
            print(f"⚠️ Skipping malformed row {i}: {row}")
            continue

        doc = {"url_id": row[0], "input_url": row[1], "status": "pending"}

        try:
            # avoid duplicates
            if not collection.find_one({"url_id": doc["url_id"]}):
                collection.insert_one(doc)
                with lock:
                    inserted_count += 1
            else:
                print(f"⚠️ Skipped duplicate url_id {doc['url_id']}")
        except Exception as e:
            print(f"❌ Error inserting row {i} in `{collection_name}`: {e}")

        if i % 1000 == 0:  # progress every 1000 rows
            print(f"✅ Inserted {i}/{len(rows)} rows into `{collection_name}`")

    print(f"🎯 Finished `{collection_name}` → {inserted_count}/{total_count} total inserted\n")

# -----------------------------
# ETL Process (Improved)
# -----------------------------
def process_all_csv_from_spaces():
    global job_running, start_time
    job_running = True
    start_time = time.time()

    print(f"\n📦 Scanning bucket `{SPACES_BUCKET}` under prefix `{SPACES_PREFIX}`...\n")
    try:
        objects = s3.list_objects_v2(Bucket=SPACES_BUCKET, Prefix=SPACES_PREFIX)
    except Exception as e:
        print(f"❌ Failed to list objects in bucket: {e}")
        job_running = False
        return

    if 'Contents' not in objects:
        print("⚠️ No CSV files found in bucket.")
        job_running = False
        return

    csv_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".csv")]
    print(f"📂 Found {len(csv_files)} CSV files\n")

    for key in csv_files:
        filename = os.path.basename(key)
        collection_name = os.path.splitext(filename)[0] if MODE == "per_file" else FIXED_COLLECTION

        print(f"\n📤 Processing `{filename}` → inserting into `{collection_name}`")

        buffer = download_with_progress(SPACES_BUCKET, key, filename)
        if buffer:
            try:
                insert_csv_from_stream(buffer, collection_name)
            except Exception as e:
                print(f"❌ Critical error while processing `{filename}`: {e}")

    job_running = False
    duration = int(time.time() - start_time)
    print(f"\n🎯 All uploads complete in {duration}s. Total inserted: {inserted_count}/{total_count}")

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

    progress = f"{(inserted_count/total_count*100):.2f}%" if total_count else "0%"
    healthy = cpu < 80 and mem.percent < 80

    # ✅ Mobile-friendly HTML with inline CSS
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{
          font-family: Arial, sans-serif;
          padding: 15px;
          background: #f9f9f9;
          color: #333;
        }}
        h2 {{
          text-align: center;
          color: #444;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin-top: 15px;
        }}
        th, td {{
          padding: 10px;
          text-align: left;
          border-bottom: 1px solid #ddd;
        }}
        tr:hover {{ background-color: #f1f1f1; }}
        .ok {{ color: green; font-weight: bold; }}
        .bad {{ color: red; font-weight: bold; }}
      </style>
    </head>
    <body>
      <h2>📊 Job Stats</h2>
      <table>
        <tr><th>Inserted</th><td>{inserted_count}</td></tr>
        <tr><th>Total</th><td>{total_count}</td></tr>
        <tr><th>Progress</th><td>{progress}</td></tr>
        <tr><th>CPU Usage</th><td>{cpu}%</td></tr>
        <tr><th>Memory Usage</th><td>{mem.percent}%</td></tr>
        <tr><th>Uptime (sec)</th><td>{uptime}</td></tr>
        <tr><th>Job Running</th><td>{"✅ Yes" if job_running else "❌ No"}</td></tr>
        <tr><th>Status</th><td class="{ 'ok' if healthy else 'bad' }">{'Healthy' if healthy else 'High Load'}</td></tr>
      </table>
    </body>
    </html>
    """
    return html

# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

