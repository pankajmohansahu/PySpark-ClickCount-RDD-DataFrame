"""
Week 4 - Graded Assignment 3
=============================
PySpark Program: Click Count by Time-of-Day Bins
Implementation : DataFrame-based approach  (NO SparkSQL)

Author  : (Your Name / Roll Number)
Date    : 2026-03-08

Description:
    Reads a CSV file (user_id, timestamp) from Google Cloud Storage,
    buckets each click into one of four 6-hour time windows using the
    PySpark DataFrame API (pyspark.sql.functions), and writes the
    aggregated counts back to GCS as a JSON file.

Time bins:
    0  – 6  hours  →  "00-06 (Night)"
    6  – 12 hours  →  "06-12 (Morning)"
    12 – 18 hours  →  "12-18 (Afternoon)"
    18 – 24 hours  →  "18-24 (Evening)"

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark gs://<BUCKET>/scripts/click_count_dataframe.py \
        --cluster=<CLUSTER_NAME> \
        --region=<REGION> \
        -- \
        --input_path=gs://<BUCKET>/input/click_file.txt \
        --output_path=gs://<BUCKET>/output/df_output
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def main(input_path, output_path):

    spark = (
        SparkSession.builder
        .appName("ClickCount_DataFrame")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read the CSV from GCS ───────────────────────────────────────────
    #       header=True   → first row treated as column names
    #       inferSchema   → auto-detect dtypes (may parse timestamp as string)
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    print(">>> Raw Schema:")
    df_raw.printSchema()
    print(f">>> Total rows (raw): {df_raw.count():,}")
    df_raw.show(5, truncate=False)

    # ── 2. Normalise column names to lowercase without spaces ─────────────
    df = df_raw.toDF(*[c.strip().lower().replace(" ", "_")
                       for c in df_raw.columns])

    # ── 3. Parse the timestamp column → extract the hour ──────────────────
    #       Try multiple common timestamp formats so the code is robust.
    #
    #       If 'timestamp' column was read as TimestampType → use F.hour()
    #       If it was read as StringType  → cast first.
    ts_col = "timestamp"

    # Force cast to TimestampType (handles "yyyy-MM-dd HH:mm:ss" and ISO-8601)
    df = df.withColumn(
        "ts_parsed",
        F.coalesce(
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp(F.col(ts_col), "MM/dd/yyyy HH:mm:ss"),
            F.to_timestamp(F.col(ts_col), "dd-MM-yyyy HH:mm:ss"),
        )
    )

    # Drop rows where timestamp could not be parsed
    df_clean = df.filter(F.col("ts_parsed").isNotNull())
    dropped   = df.count() - df_clean.count()
    if dropped > 0:
        print(f">>> WARNING: {dropped:,} rows dropped due to unparseable timestamps.")

    # ── 4. Extract the hour (0–23) ────────────────────────────────────────
    df_with_hour = df_clean.withColumn("hour", F.hour(F.col("ts_parsed")))

    # ── 5. Assign time-of-day bin label using when / otherwise ───────────
    #       NOTE: No SparkSQL used — purely DataFrame API
    df_binned = df_with_hour.withColumn(
        "time_bin",
        F.when((F.col("hour") >= 0)  & (F.col("hour") < 6),  "00-06 (Night)")
         .when((F.col("hour") >= 6)  & (F.col("hour") < 12), "06-12 (Morning)")
         .when((F.col("hour") >= 12) & (F.col("hour") < 18), "12-18 (Afternoon)")
         .otherwise("18-24 (Evening)")
    )

    # ── 6. Count clicks per bin (groupBy + count — DataFrame API only) ────
    df_counts = (
        df_binned
        .groupBy("time_bin")
        .count()
        .withColumnRenamed("count", "click_count")
        .orderBy("time_bin")              # sort alphabetical = chronological
    )

    # ── 7. Display results on Driver logs ─────────────────────────────────
    print("\n" + "=" * 55)
    print("  Click Count by Time-of-Day Bin  (DataFrame API)")
    print("=" * 55)
    df_counts.show(truncate=False)
    print("=" * 55 + "\n")

    # ── 8. Write output JSON to GCS ────────────────────────────────────────
    df_counts.write \
             .mode("overwrite") \
             .format("json") \
             .save(output_path)

    print(f">>> Output written to: {output_path}")

    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Click count by time-of-day bins — DataFrame approach"
    )
    parser.add_argument(
        "--input_path",
        required=True,
        help="GCS path to input file, e.g. gs://bucket/input/click_file.txt"
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="GCS path for output JSON, e.g. gs://bucket/output/df_output"
    )

    args = parser.parse_args()
    main(args.input_path, args.output_path)
