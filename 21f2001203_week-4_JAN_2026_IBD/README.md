# IIT Madras — Introduction to Big Data (IBD)
## Week 4 — Graded Assignment 3
### Click Count by Time-of-Day Bins using PySpark on Google Cloud Dataproc

---

## Table of Contents

1. [Assignment Overview](#1-assignment-overview)
2. [Problem Statement](#2-problem-statement)
3. [Time-of-Day Bins](#3-time-of-day-bins)
4. [Repository / Folder Structure](#4-repository--folder-structure)
5. [Input Data](#5-input-data)
6. [Architecture & GCP Pipeline](#6-architecture--gcp-pipeline)
7. [Implementation Approaches](#7-implementation-approaches)
   - [Approach 1 — RDD-based (`click_count_rdd.py`)](#approach-1--rdd-based-click_count_rddpy)
   - [Approach 2 — DataFrame API (`click_count_dataframe.py`)](#approach-2--dataframe-api-click_count_dataframepy)
   - [Combined Script (`click_count_combined.py`)](#combined-script-click_count_combinedpy)
8. [GCP Setup — Step-by-Step](#8-gcp-setup--step-by-step)
9. [Running the Jobs on Dataproc](#9-running-the-jobs-on-dataproc)
10. [Expected Output](#10-expected-output)
11. [Output Files](#11-output-files)
12. [Key PySpark Concepts Used](#12-key-pyspark-concepts-used)
13. [RDD vs DataFrame — Comparison](#13-rdd-vs-dataframe--comparison)
14. [Timestamp Formats Supported](#14-timestamp-formats-supported)
15. [Submission Structure](#15-submission-structure)

---

## 1. Assignment Overview

| Field            | Details                                                      |
|------------------|--------------------------------------------------------------|
| **Course**       | Introduction to Big Data (IBD)                               |
| **Institute**    | IIT Madras — BS in Data Science & Applications               |
| **Term**         | January 2026 Term                                            |
| **Assignment**   | Week 4 — Graded Assignment 3 (GA3)                           |
| **Topic**        | Click Count by Time-of-Day Bins using PySpark                |
| **Platform**     | Google Cloud Platform (GCP) — Dataproc + GCS                 |
| **Language**     | Python 3 with PySpark                                        |
| **Date**         | 2026-03-08                                                   |

---

## 2. Problem Statement

Given a text/CSV file stored in **Google Cloud Storage (GCS)** with two columns:

```
user_id , timestamp
```

The goal is to:

1. **Read** the file from GCS into Apache Spark.
2. **Parse** the timestamp of each click event.
3. **Categorise** each click into one of four 6-hour time-of-day bins.
4. **Count** the total number of clicks in each bin.
5. **Write** the result back to GCS as a JSON file.
6. Implement the solution **twice**:
   - Once using **PySpark RDDs** (low-level API)
   - Once using the **PySpark DataFrame API** (high-level API, NO SparkSQL)
7. Run everything on a **Google Cloud Dataproc** cluster.

> **Important:** SparkSQL (i.e., `spark.sql("SELECT ...")`) is **NOT used** anywhere in this assignment. All aggregations are performed using the RDD API or the DataFrame API (`pyspark.sql.functions`).

---

## 3. Time-of-Day Bins

Each click timestamp is mapped to one of four non-overlapping 6-hour windows:

| Hour Range (24h) | Bin Label            | Period    |
|------------------|----------------------|-----------|
| `00:00 – 05:59`  | `00-06 (Night)`      | Night     |
| `06:00 – 11:59`  | `06-12 (Morning)`    | Morning   |
| `12:00 – 17:59`  | `12-18 (Afternoon)`  | Afternoon |
| `18:00 – 23:59`  | `18-24 (Evening)`    | Evening   |

Mapping rule (using Python comparison):

```
hour  in [0,  6)  →  "00-06 (Night)"
hour  in [6, 12)  →  "06-12 (Morning)"
hour  in [12, 18) →  "12-18 (Afternoon)"
hour  in [18, 24) →  "18-24 (Evening)"
```

---

## 4. Repository / Folder Structure

```
Week-4/
│
├── click_count_rdd.py            ← Approach 1: Pure RDD implementation
├── click_count_dataframe.py      ← Approach 2: DataFrame API implementation
├── click_count_combined.py       ← Both approaches in one script (comparison mode)
│
├── click_file.txt                ← Main input dataset (200 rows, CSV format)
├── sample_click_data.txt         ← Smaller sample dataset (29 rows, for local testing)
├── VIDEO_SCRIPT.txt              ← Script for the video demonstration
│
├── README.md                     ← This file
│
└── 21f2001203_week-4_JAN_2026_IBD/   ← Official submission folder
    ├── click_count_dataframe.py
    ├── click_count_rdd.py
    ├── click_file.txt
    ├── output_files/
    │   ├── dataframe_output.json.json   ← Output from DataFrame approach
    │   └── rdd_output.json.json         ← Output from RDD approach
    └── Screen_Shot/                     ← Screenshots of GCP Console & results
```

---

## 5. Input Data

### File: `click_file.txt`

- **Format:** CSV with header row
- **Columns:** `user_id`, `timestamp`
- **Total Rows:** 200 click events (+ 1 header)
- **Date Coverage:** 2025-08-01 (single day)
- **Users:** 50 unique users (`u001` – `u050`)
- **Timestamp Format:** `yyyy-MM-dd HH:mm:ss`

**Sample rows:**

```csv
user_id,timestamp
u001,2025-08-01 09:03:23
u012,2025-08-01 12:37:50
u006,2025-08-01 06:57:32
u006,2025-08-01 21:09:33
u029,2025-08-01 14:00:44
u027,2025-08-01 00:52:08
```

### File: `sample_click_data.txt`

- **Format:** CSV with header row
- **Columns:** `user_id`, `timestamp`
- **Total Rows:** 29 (useful for quick local testing)

**Sample rows:**

```csv
user_id,timestamp
user_001,2024-01-15 01:23:45
user_002,2024-01-15 02:45:10
user_005,2024-01-15 06:00:01
user_011,2024-01-15 12:00:05
user_017,2024-01-15 18:00:01
```

---

## 6. Architecture & GCP Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                    GOOGLE CLOUD PLATFORM                         │
│                                                                  │
│  ┌─────────────────────────┐      ┌───────────────────────────┐  │
│  │  Cloud Storage (GCS)    │      │   Dataproc Cluster        │  │
│  │                         │      │                           │  │
│  │  Bucket:                │      │   Cluster Name:           │  │
│  │  ibd-week4-21f2001203   │      │   ibd-week4-cluster       │  │
│  │                         │      │                           │  │
│  │  input/                 │─────▶│   Master Node (1x)        │  │
│  │    click_file.txt       │      │   n1-standard-2           │  │
│  │                         │      │                           │  │
│  │  scripts/               │─────▶│   Worker Nodes (2x)       │  │
│  │    click_count_rdd.py   │      │   n1-standard-2           │  │
│  │    click_count_df.py    │      │                           │  │
│  │                         │      │   PySpark Job:            │  │
│  │  output/                │◀─────│   ┌──────────────────┐    │  │
│  │    rdd_output/          │      │   │  RDD Approach    │    │  │
│  │    df_output/           │      │   │  DF  Approach    │    │  │
│  │                         │      │   └──────────────────┘    │  │
│  └─────────────────────────┘      └───────────────────────────┘  │
│                                                                  │
│   Region: us-central1   |   Zone: us-central1-a                  │
│   Dataproc Image: 2.2.77-debian12                                │
└──────────────────────────────────────────────────────────────────┘
```

**Data Flow:**

```
GCS (input CSV)
    │
    ▼
Spark reads file into memory
    │
    ├─── RDD Pipeline ──────────────────────────────────────────────────┐
    │    textFile() → filter(header) → map(parse+bin) →                 │
    │    filter(None) → reduceByKey(+) → sortByKey()                    │
    │                                                                   │
    └─── DataFrame Pipeline ────────────────────────────────────────────┘
         read.csv() → toDF(normalize cols) → withColumn(ts_parsed) →
         withColumn(hour) → withColumn(time_bin) → groupBy.count() →
         withColumnRenamed → orderBy
    │
    ▼
GCS (output JSON)
```

---

## 7. Implementation Approaches

### Approach 1 — RDD-based (`click_count_rdd.py`)

This implementation uses Spark's **low-level RDD API** — the foundational distributed data structure in Apache Spark.

#### Pipeline Steps:

| Step | Operation         | Description                                             |
|------|-------------------|---------------------------------------------------------|
| 1    | `textFile()`      | Load the CSV from GCS as an RDD of raw text lines       |
| 2    | `.first()`        | Read the header line to detect column positions         |
| 3    | `.filter()`       | Remove the header row and blank lines                   |
| 4    | `.map(parse)`     | Split each line by comma, extract the timestamp, parse the hour, return `(bin_label, 1)` |
| 5    | `.filter(None)`   | Discard rows with malformed/unparseable timestamps      |
| 6    | `reduceByKey(+)`  | Sum the counts per bin label (classic word-count pattern) |
| 7    | `sortByKey()`     | Sort bins alphabetically (which equals chronological order) |
| 8    | `.collect()`      | Pull results to the driver and print them               |
| 9    | `createDataFrame` | Wrap the RDD in a DataFrame to use `.write.json()` for GCS output |

#### Key Helper Function — `get_time_bin()`:

```python
def get_time_bin(timestamp_str):
    """Extract the hour from a timestamp string and return the bin label."""
    try:
        ts = timestamp_str.strip()
        if "T" in ts:
            ts = ts.replace("T", " ")       # normalize ISO-8601 'T' separator

        time_part = ts.split(" ")[1] if " " in ts else ts
        hour = int(time_part.split(":")[0])

        if 0 <= hour < 6:
            return "00-06 (Night)"
        elif 6 <= hour < 12:
            return "06-12 (Morning)"
        elif 12 <= hour < 18:
            return "12-18 (Afternoon)"
        else:
            return "18-24 (Evening)"
    except Exception:
        return None    # malformed rows are filtered out downstream
```

#### RDD Code Flow (condensed):

```python
raw_rdd    = sc.textFile(input_path)
header     = raw_rdd.first()
data_rdd   = raw_rdd.filter(lambda line: line != header and line.strip() != "")
parsed_rdd = data_rdd.map(parse_and_bin).filter(lambda x: x is not None)
counts_rdd = parsed_rdd.reduceByKey(lambda a, b: a + b)
sorted_rdd = counts_rdd.sortByKey()
```

---

### Approach 2 — DataFrame API (`click_count_dataframe.py`)

This implementation uses Spark's **high-level DataFrame API** with `pyspark.sql.functions`. **No SparkSQL (`spark.sql(...)`) is used.**

#### Pipeline Steps:

| Step | Operation                | Description                                                    |
|------|--------------------------|----------------------------------------------------------------|
| 1    | `spark.read.csv()`       | Load CSV from GCS with auto-detected schema and header         |
| 2    | `.toDF(*cols)`           | Normalize column names (lowercase, replace spaces with `_`)    |
| 3    | `F.coalesce(F.to_timestamp(...))` | Try multiple timestamp formats, parse to `TimestampType`   |
| 4    | `.filter(isNotNull)`     | Drop rows where timestamp could not be parsed                  |
| 5    | `F.hour()`               | Extract integer hour (0–23) from the parsed timestamp          |
| 6    | `F.when(...).otherwise()`| Assign the `time_bin` label based on the hour                  |
| 7    | `.groupBy("time_bin").count()` | Count clicks per bin                                     |
| 8    | `.withColumnRenamed()`   | Rename `count` → `click_count`                                 |
| 9    | `.orderBy("time_bin")`   | Sort bins alphabetically (= chronological)                     |
| 10   | `.write.json()`          | Save results to GCS as JSON                                    |

#### Timestamp Parsing Strategy — `F.coalesce()`:

Multiple timestamp formats are tried in order. The first format that successfully parses is used:

```python
F.coalesce(
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),       # most common
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),    # ISO-8601
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"),  # with millis
    F.to_timestamp(F.col("timestamp"), "MM/dd/yyyy HH:mm:ss"),      # US format
    F.to_timestamp(F.col("timestamp"), "dd-MM-yyyy HH:mm:ss"),      # EU format
)
```

If none match, the row is dropped with a warning.

#### Bin Assignment using `when / otherwise`:

```python
df_binned = df_with_hour.withColumn(
    "time_bin",
    F.when((F.col("hour") >= 0)  & (F.col("hour") < 6),  "00-06 (Night)")
     .when((F.col("hour") >= 6)  & (F.col("hour") < 12), "06-12 (Morning)")
     .when((F.col("hour") >= 12) & (F.col("hour") < 18), "12-18 (Afternoon)")
     .otherwise("18-24 (Evening)")
)
```

---

### Combined Script (`click_count_combined.py`)

This is a single script that runs **both approaches sequentially** using the same SparkSession and prints a side-by-side comparison to verify that both methods produce identical results.

**Usage:**

```bash
gcloud dataproc jobs submit pyspark gs://<BUCKET>/scripts/click_count_combined.py \
    --cluster=<CLUSTER_NAME> \
    --region=<REGION> \
    -- \
    --input_path=gs://<BUCKET>/input/click_file.txt \
    --rdd_output=gs://<BUCKET>/output/rdd_output \
    --df_output=gs://<BUCKET>/output/df_output
```

**Final Comparison Output (printed to driver log):**

```
════════════════════════════════════════════════════════════
  FINAL COMPARISON — RDD vs DataFrame
════════════════════════════════════════════════════════════

  Time Bin                   RDD Count    DF Count
  ────────────────────────────────────────────────────
  00-06 (Night)                     44          44   ✓
  06-12 (Morning)                   53          53   ✓
  12-18 (Afternoon)                 48          48   ✓
  18-24 (Evening)                   55          55   ✓
  ────────────────────────────────────────────────────

  Both approaches must produce identical counts.
```

---

## 8. GCP Setup — Step-by-Step

### Step 1 — Enable APIs

In the GCP Console, go to **APIs & Services** and enable:
- Cloud Dataproc API
- Cloud Storage API

### Step 2 — Create a GCS Bucket

```bash
gsutil mb -l us-central1 gs://ibd-week4-21f2001203
```

Create the folder structure inside the bucket:

```bash
gsutil cp click_file.txt  gs://ibd-week4-21f2001203/input/click_file.txt
gsutil cp click_count_rdd.py          gs://ibd-week4-21f2001203/scripts/
gsutil cp click_count_dataframe.py    gs://ibd-week4-21f2001203/scripts/
gsutil cp click_count_combined.py     gs://ibd-week4-21f2001203/scripts/
```

**Bucket layout after setup:**

```
gs://ibd-week4-21f2001203/
├── input/
│   └── click_file.txt
├── scripts/
│   ├── click_count_rdd.py
│   ├── click_count_dataframe.py
│   └── click_count_combined.py
└── output/              ← created automatically by Spark when job runs
    ├── rdd_output/
    └── df_output/
```

### Step 3 — Create a Dataproc Cluster

```bash
gcloud dataproc clusters create ibd-week4-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --image-version=2.2-debian12 \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --num-workers=2 \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=50GB
```

**Cluster Configuration Summary:**

| Parameter        | Value                   |
|------------------|-------------------------|
| Cluster Name     | `ibd-week4-cluster`     |
| Region           | `us-central1`           |
| Zone             | `us-central1-a`         |
| Image Version    | `2.2-debian12`          |
| Master Node      | 1× n1-standard-2        |
| Worker Nodes     | 2× n1-standard-2        |
| Master Disk      | 50 GB                   |
| Worker Disk      | 50 GB                   |

---

## 9. Running the Jobs on Dataproc

### Run the RDD Approach

```bash
gcloud dataproc jobs submit pyspark \
    gs://ibd-week4-21f2001203/scripts/click_count_rdd.py \
    --cluster=ibd-week4-cluster \
    --region=us-central1 \
    -- \
    --input_path=gs://ibd-week4-21f2001203/input/click_file.txt \
    --output_path=gs://ibd-week4-21f2001203/output/rdd_output
```

### Run the DataFrame Approach

```bash
gcloud dataproc jobs submit pyspark \
    gs://ibd-week4-21f2001203/scripts/click_count_dataframe.py \
    --cluster=ibd-week4-cluster \
    --region=us-central1 \
    -- \
    --input_path=gs://ibd-week4-21f2001203/input/click_file.txt \
    --output_path=gs://ibd-week4-21f2001203/output/df_output
```

### Run the Combined Script (both at once)

```bash
gcloud dataproc jobs submit pyspark \
    gs://ibd-week4-21f2001203/scripts/click_count_combined.py \
    --cluster=ibd-week4-cluster \
    --region=us-central1 \
    -- \
    --input_path=gs://ibd-week4-21f2001203/input/click_file.txt \
    --rdd_output=gs://ibd-week4-21f2001203/output/rdd_output \
    --df_output=gs://ibd-week4-21f2001203/output/df_output
```

### Verify Output Files

```bash
# List output files
gsutil ls gs://ibd-week4-21f2001203/output/rdd_output/
gsutil ls gs://ibd-week4-21f2001203/output/df_output/

# View the JSON output
gsutil cat gs://ibd-week4-21f2001203/output/rdd_output/part-*.json
gsutil cat gs://ibd-week4-21f2001203/output/df_output/part-*.json
```

### Delete the Cluster (to stop billing)

```bash
gcloud dataproc clusters delete ibd-week4-cluster \
    --region=us-central1 \
    --quiet
```

---

## 10. Expected Output

Both the RDD and DataFrame approaches produce identical results for the 200-row `click_file.txt`:

| Time Bin            | Click Count | % of Total |
|---------------------|-------------|------------|
| `00-06 (Night)`     | 44          | 22.0%      |
| `06-12 (Morning)`   | 53          | 26.5%      |
| `12-18 (Afternoon)` | 48          | 24.0%      |
| `18-24 (Evening)`   | 55          | 27.5%      |
| **TOTAL**           | **200**     | **100%**   |

**Console output from the RDD approach:**

```
==================================================
  Click Count by Time-of-Day Bin  (RDD)
==================================================
  00-06 (Night)             :        44 clicks
  06-12 (Morning)           :        53 clicks
  12-18 (Afternoon)         :        48 clicks
  18-24 (Evening)           :        55 clicks
==================================================
```

**Console output from the DataFrame approach:**

```
=======================================================
  Click Count by Time-of-Day Bin  (DataFrame API)
=======================================================
+--------------------+-----------+
|time_bin            |click_count|
+--------------------+-----------+
|00-06 (Night)       |44         |
|06-12 (Morning)     |53         |
|12-18 (Afternoon)   |48         |
|18-24 (Evening)     |55         |
+--------------------+-----------+
=======================================================
```

---

## 11. Output Files

Spark writes output in a **partitioned format** (one or more `part-*.json` files) inside the output directory.

### `rdd_output.json.json` (local copy in submission folder)

```json
{"time_bin":"00-06 (Night)","click_count":44}
{"time_bin":"06-12 (Morning)","click_count":53}
{"time_bin":"12-18 (Afternoon)","click_count":48}
{"time_bin":"18-24 (Evening)","click_count":55}
```

### `dataframe_output.json.json` (local copy in submission folder)

```json
{"time_bin":"00-06 (Night)","click_count":44}
{"time_bin":"06-12 (Morning)","click_count":53}
{"time_bin":"12-18 (Afternoon)","click_count":48}
{"time_bin":"18-24 (Evening)","click_count":55}
```

Both files contain identical data, confirming that both approaches produce the same result.

---

## 12. Key PySpark Concepts Used

### RDD Approach

| Concept                  | API Used                    | Purpose                                              |
|--------------------------|-----------------------------|------------------------------------------------------|
| Create RDD from file     | `sc.textFile(path)`         | Load the CSV as raw text lines                       |
| Detect header            | `rdd.first()`               | Get the first line (column names)                    |
| Filter rows              | `rdd.filter(lambda ...)`    | Remove header and blank lines                        |
| Transform each line      | `rdd.map(func)`             | Parse and produce `(bin_label, 1)` key-value pairs   |
| Remove invalid rows      | `rdd.filter(None check)`    | Drop rows where timestamp parsing failed             |
| Aggregate by key         | `rdd.reduceByKey(lambda a, b: a + b)` | Sum click counts per time bin           |
| Sort by key              | `rdd.sortByKey()`           | Sort bins in chronological order                     |
| Collect to driver        | `rdd.collect()`             | Pull all results to print in driver logs             |
| Save as JSON             | `df.write.format("json")...`| Persist results to GCS                              |

### DataFrame Approach

| Concept                  | API Used                          | Purpose                                         |
|--------------------------|-----------------------------------|-------------------------------------------------|
| Read CSV                 | `spark.read.csv(...)`             | Load the CSV with header and type inference     |
| Rename columns           | `df.toDF(*cols)`                  | Normalize column names                          |
| Parse timestamp          | `F.to_timestamp(col, format)`     | Convert string to `TimestampType`               |
| Fallback parsing         | `F.coalesce(...)`                 | Try multiple formats, use first that works      |
| Drop nulls               | `df.filter(col.isNotNull())`      | Remove rows with unparseable timestamps         |
| Extract hour             | `F.hour(col)`                     | Get integer hour from a timestamp               |
| Conditional column       | `F.when(...).when(...).otherwise()`| Assign bin labels based on hour ranges         |
| Group and count          | `df.groupBy("col").count()`       | Count clicks per time bin                       |
| Rename column            | `.withColumnRenamed("count", ...)` | Rename the aggregated count column             |
| Sort                     | `.orderBy("col")`                 | Order results chronologically                   |
| Write JSON               | `df.write.mode("overwrite").format("json").save(path)` | Persist to GCS     |

---

## 13. RDD vs DataFrame — Comparison

| Aspect               | RDD Approach                              | DataFrame Approach                              |
|----------------------|-------------------------------------------|-------------------------------------------------|
| **API Level**        | Low-level (functional, lambda-based)      | High-level (declarative, SQL-like)              |
| **Timestamp Parsing**| Manual Python string splitting            | `F.to_timestamp()` with `F.coalesce()`          |
| **Binning Logic**    | Python `if/elif/else` inside a `map()`   | `F.when(...).otherwise()` column expression     |
| **Aggregation**      | `reduceByKey(lambda a, b: a + b)`        | `groupBy(...).count()`                          |
| **Sorting**          | `sortByKey()`                             | `orderBy(...)`                                  |
| **Type Safety**      | None (all strings)                        | Schema-enforced (TimestampType, IntegerType)    |
| **Optimization**     | None (developer controlled)              | Catalyst optimizer + Tungsten                   |
| **Readability**      | Verbose, more boilerplate                 | Concise, easier to understand                   |
| **Performance**      | Slower for structured data               | Faster due to query planning and columnar format |
| **Use Case**         | Unstructured / complex custom parsing    | Structured / tabular data                       |
| **Result**           | **Identical counts**                      | **Identical counts**                            |

---

## 14. Timestamp Formats Supported

Both the RDD and DataFrame implementations support the following timestamp formats:

| Format String              | Example                    | Notes                       |
|----------------------------|----------------------------|-----------------------------|
| `yyyy-MM-dd HH:mm:ss`      | `2025-08-01 09:03:23`      | Most common (used in input) |
| `yyyy-MM-dd'T'HH:mm:ss`    | `2025-08-01T09:03:23`      | ISO 8601 with T separator   |
| `yyyy-MM-dd HH:mm:ss.SSS`  | `2025-08-01 09:03:23.000`  | With milliseconds           |
| `MM/dd/yyyy HH:mm:ss`      | `08/01/2025 09:03:23`      | US date format              |
| `dd-MM-yyyy HH:mm:ss`      | `01-08-2025 09:03:23`      | European date format        |
| `HH:mm:ss`                 | `09:03:23`                 | Time-only (RDD only)        |

Rows with timestamps that cannot be parsed in any of the above formats are **silently dropped** with a warning printed to the driver log.

---

## 15. Submission Structure

The official submission folder for this assignment is:

```
21f2001203_week-4_JAN_2026_IBD/
├── click_count_dataframe.py        ← DataFrame API solution
├── click_count_rdd.py              ← RDD solution
├── click_file.txt                  ← Input data file
├── output_files/
│   ├── dataframe_output.json.json  ← JSON output from DataFrame approach
│   └── rdd_output.json.json        ← JSON output from RDD approach
└── Screen_Shot/                    ← Screenshots of:
                                        - GCS bucket with uploaded files
                                        - Dataproc cluster details
                                        - Dataproc job submission & logs
                                        - JSON output files on GCS
```

---

## Dependencies & Environment

| Component       | Version / Details                         |
|-----------------|-------------------------------------------|
| Python          | 3.x (provided by Dataproc image)          |
| PySpark         | Bundled with Dataproc 2.2 (Spark 3.3+)   |
| Dataproc Image  | 2.2-debian12                              |
| Google Cloud SDK| `gcloud` CLI for job submission           |
| Storage         | Google Cloud Storage (GCS)                |

No additional Python packages are required — only the standard PySpark library included in the Dataproc runtime is used.

---

## Notes

- The output directory on GCS is created automatically by Spark when the job runs. If the directory already exists, `mode("overwrite")` replaces the previous output.
- Spark writes output as **multiple part files** (e.g., `part-00000-*.json`). The JSON files in the `output_files/` folder in this repository are the merged/renamed copies of those part files for easy viewing.
- The `SparkSession` is created with `.getOrCreate()` so that the same session is safely reused when running both approaches (as in `click_count_combined.py`) without triggering a "multiple SparkContext" error.
- Log level is set to `WARN` (`spark.sparkContext.setLogLevel("WARN")`) to suppress verbose INFO logs and make the job output readable.
- The `--` separator in the `gcloud dataproc jobs submit pyspark` command is **required** — it separates `gcloud` flags from the PySpark script's own `argparse` arguments.
