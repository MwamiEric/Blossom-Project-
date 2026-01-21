import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# Kobo credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aWiaZSvm6LN3kabZFfNcDm/export-settings/esMcpnfX4xJHguGJm3zdqeW/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Schema and table
schema_name = "ai_uses_and_its_ethics"
table_name = "ai_uses_and_its_ethics"

# -----------------------------
# Step 1: Fetch data from Kobo
# -----------------------------
print("Fetching data from KoboToolbox...")
response = requests.get(
    KOBO_CSV_URL,
    auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD)
)

if response.status_code != 200:
    raise Exception(f"Failed to fetch data: {response.status_code}")

print("✅ Data fetched successfully")

df = pd.read_csv(io.StringIO(response.text), sep=";", on_bad_lines="skip")

# -----------------------------
# Step 2: Clean column names
# -----------------------------
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("&", "and")
    .str.replace("-", "_")
)

# -----------------------------
# Step 3: Convert date columns
# -----------------------------
df["start"] = pd.to_datetime(df["start"], errors="coerce")
df["end"] = pd.to_datetime(df["end"], errors="coerce")
df["_submission_time"] = pd.to_datetime(df["_submission_time"], errors="coerce")

# -----------------------------
# Step 4: Connect to PostgreSQL
# -----------------------------
print("Uploading data to PostgreSQL...")

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)

cur = conn.cursor()

# -----------------------------
# Step 5: Create schema & table
# -----------------------------
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

cur.execute(f"""
CREATE TABLE {schema_name}.{table_name} (
    id SERIAL PRIMARY KEY,
    start TIMESTAMP,
    "end" TIMESTAMP,
    names TEXT,
    email TEXT,
    phone TEXT,
    consent TEXT,
    age INT,
    gender TEXT,
    university TEXT,
    field TEXT,
    use_of_ai TEXT,
    policy TEXT,
    frequency TEXT,
    dependent TEXT,
    cirriculum TEXT,
    submission_time TIMESTAMP,
    submitted_by TEXT,
    status TEXT,
    version TEXT,
    index_no INTEGER
);
""")

# -----------------------------
# Step 6: Insert data
# -----------------------------
insert_query = f"""
INSERT INTO {schema_name}.{table_name} (
    start,
    "end",
    names,
    email,
    phone,
    consent,
    age,
    gender,
    university,
    field,
    use_of_ai,
    policy,
    frequency,
    dependent,
    cirriculum,
    submission_time,
    submitted_by,
    status,
    version,
    index_no
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
);
"""

for _, row in df.iterrows():
    cur.execute(insert_query, (
        row.get("start"),
        row.get("end"),
        row.get("names"),
        row.get("email"),
        row.get("phone"),
        row.get("consent"),
        row.get("age"),
        row.get("gender"),
        row.get("university"),
        row.get("field"),
        row.get("use_of_ai"),
        row.get("policy"),
        row.get("frequency"),
        row.get("dependent"),
        row.get("cirriculum"),
        row.get("_submission_time"),
        row.get("_submitted_by"),
        row.get("_status"),
        row.get("__version__"),
        row.get("_index")
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Data successfully loaded into PostgreSQL!")
