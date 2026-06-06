# PySpark Click Count Analysis — RDD & DataFrame API

**Course:** Introduction to Big Data (IBD) — IIT Madras BS in Data Science & Applications  
**Assignment:** Week 4 — Graded Assignment 3  
**Term:** January 2026  
**Platform:** Google Cloud Dataproc + Google Cloud Storage (GCS)

---

## Problem Statement

Given a dataset of user click events (each row containing a `user_id` and a `timestamp`), compute the **total number of clicks** that fall into each of the following **four 6-hour time-of-day bins**:

| Time Range | Bin Label         |
|------------|-------------------|
| 00 – 06 h  | 00-06 (Night)     |
| 06 – 12 h  | 06-12 (Morning)   |
| 12 – 18 h  | 12-18 (Afternoon) |
| 18 – 24 h  | 18-24 (Evening)   |

The solution is implemented **twice** — once using the **PySpark RDD API** and once using the **PySpark DataFrame API** (SparkSQL is intentionally not used).

---

## Repository Structure

```
├── click_count_rdd.py          # Implementation using PySpark RDD API
├── click_count_dataframe.py    # Implementation using PySpark DataFrame API
├── click_count_combined.py     # Both approaches in a single script with side-by-side comparison
├── click_file.txt              # Input dataset (200 rows: user_id, timestamp)
├── sample_click_data.txt       # Sample data for local testing
└── README.md
```

---

## Input Data Format

File: `click_file.txt`  
200 rows, comma-separated, with a header row.

```
user_id,timestamp
u001,2025-08-01 09:03:23
u012,2025-08-01 12:37:50
u006,2025-08-01 06:57:32
u006,2025-08-01 21:09:33
...
```

---

## Implementations

### 1. RDD-based — `click_count_rdd.py`

Uses the low-level Spark RDD API following a classic **MapReduce** pattern:

```
textFile()
  → filter header
  → map (parse timestamp → bin label, emit (bin, 1))
  → filter malformed rows
  → reduceByKey (sum counts per bin)
  → sortByKey (alphabetical = chronological)
  → write JSON to GCS
```

Key functions used: `sc.textFile()`, `.map()`, `.filter()`, `.reduceByKey()`, `.sortByKey()`, `.collect()`

---

### 2. DataFrame API-based — `click_count_dataframe.py`

Uses the high-level PySpark DataFrame API — **no SparkSQL**:

```
spark.read.csv()
  → F.coalesce(F.to_timestamp(...))   # robust multi-format timestamp parsing
  → F.hour()                          # extract hour from parsed timestamp
  → F.when().otherwise()              # assign bin label
  → groupBy("time_bin").count()       # aggregate
  → orderBy()                         # sort
  → write JSON to GCS
```

Key functions used: `spark.read.csv()`, `F.to_timestamp()`, `F.hour()`, `F.when()`, `.groupBy()`, `.count()`, `.orderBy()`

---

### 3. Combined — `click_count_combined.py`

Runs both approaches sequentially in a single Spark session and prints a **side-by-side comparison** to verify that both approaches produce identical results.

---

## Output Results

Both approaches produce identical output:

```
+--------------------+-----------+
| time_bin           | click_count|
+--------------------+-----------+
| 00-06 (Night)      |     44    |
| 06-12 (Morning)    |     53    |
| 12-18 (Afternoon)  |     48    |
| 18-24 (Evening)    |     55    |
+--------------------+-----------+
Total: 200 clicks (all rows processed, 0 errors)
```

Output is saved as JSON files to GCS.

---

## GCP Infrastructure

| Component        | Configuration                        |
|------------------|--------------------------------------|
| GCS Bucket       | `ibd-week4-21f2001203`               |
| Bucket Region    | US (multi-region)                    |
| Dataproc Cluster | `ibd-week4-cluster`                  |
| Cluster Region   | `us-central1`                        |
| Image Version    | `2.2.77-debian12`                    |
| Master Node      | 1 × n1-standard-2                   |
| Worker Nodes     | 2 × n1-standard-2                   |

**GCS Folder layout:**
```
gs://ibd-week4-21f2001203/
├── input/
│   └── click_file.txt
├── scripts/
│   ├── click_count_rdd.py
│   ├── click_count_dataframe.py
│   └── click_count_combined.py
└── output/
    ├── rdd_output/
    └── df_output/
```

---

## How to Run on Google Cloud Dataproc

### Step 1 — Upload files to GCS

```bash
gsutil cp click_file.txt gs://ibd-week4-21f2001203/input/
gsutil cp click_count_rdd.py gs://ibd-week4-21f2001203/scripts/
gsutil cp click_count_dataframe.py gs://ibd-week4-21f2001203/scripts/
gsutil cp click_count_combined.py gs://ibd-week4-21f2001203/scripts/
```

### Step 2 — Submit RDD job

```bash
gcloud dataproc jobs submit pyspark \
    gs://ibd-week4-21f2001203/scripts/click_count_rdd.py \
    --cluster=ibd-week4-cluster \
    --region=us-central1 \
    -- \
    --input_path=gs://ibd-week4-21f2001203/input/click_file.txt \
    --output_path=gs://ibd-week4-21f2001203/output/rdd_output
```

### Step 3 — Submit DataFrame job

```bash
gcloud dataproc jobs submit pyspark \
    gs://ibd-week4-21f2001203/scripts/click_count_dataframe.py \
    --cluster=ibd-week4-cluster \
    --region=us-central1 \
    -- \
    --input_path=gs://ibd-week4-21f2001203/input/click_file.txt \
    --output_path=gs://ibd-week4-21f2001203/output/df_output
```

### Step 4 — Submit Combined job (both approaches + comparison)

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

---

## Tech Stack

- **Apache Spark** (PySpark) — distributed data processing
- **Google Cloud Dataproc** — managed Spark cluster
- **Google Cloud Storage (GCS)** — input/output storage
- **Python 3** — scripting language

---

## Key Concepts Demonstrated

- **RDD API:** `textFile`, `map`, `filter`, `reduceByKey`, `sortByKey` — classic MapReduce word-count pattern applied to time-bin aggregation
- **DataFrame API:** `read.csv`, `to_timestamp`, `F.hour`, `F.when/otherwise`, `groupBy`, `count`, `orderBy` — declarative, SQL-like transformations without SparkSQL
- **Robustness:** multi-format timestamp parsing, header detection, malformed row handling
- **Validation:** both approaches run in the same session and results are compared side-by-side
