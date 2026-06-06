"""
Week 4 - Graded Assignment 3
=============================
IIT Madras BS in Data Science - Intro to Big Data
Subject : Introduction to Big Data (IBD)

PySpark Program : Click Count by Time-of-Day Bins
Implementations : (1) RDD-based  |  (2) DataFrame API-based
NOTE            : SparkSQL is intentionally NOT used in this assignment.

Author          : (Your Full Name)
Roll Number     : (Your Roll Number)
Student Email   : (Your IIT-M Email)
Date            : 2026-03-08

─────────────────────────────────────────────────────────────────────────────
PROBLEM STATEMENT
─────────────────────────────────────────────────────────────────────────────
Given a text/CSV file stored in GCS with columns:
    user_id  ,  timestamp

Compute the total number of clicks that occurred in each 6-hour time bin:
    ┌─────────────┬────────────────────┐
    │   Bin Range │   Bin Label        │
    ├─────────────┼────────────────────┤
    │  00 – 06 h  │  00-06 (Night)     │
    │  06 – 12 h  │  06-12 (Morning)   │
    │  12 – 18 h  │  12-18 (Afternoon) │
    │  18 – 24 h  │  18-24 (Evening)   │
    └─────────────┴────────────────────┘

─────────────────────────────────────────────────────────────────────────────
ARCHITECTURE (GCP Pipeline)
─────────────────────────────────────────────────────────────────────────────
  [GCS bucket]  ──read──▶  [Dataproc / PySpark]  ──write──▶  [GCS bucket]
   input CSV                  - RDD approach                   rdd_output/
                              - DataFrame approach              df_output/

─────────────────────────────────────────────────────────────────────────────
USAGE — submit via gcloud CLI:
─────────────────────────────────────────────────────────────────────────────

  # Upload this script to GCS first:
  gsutil cp click_count_combined.py gs://<YOUR_BUCKET>/scripts/

  # Submit the Dataproc job:
  gcloud dataproc jobs submit pyspark gs://<YOUR_BUCKET>/scripts/click_count_combined.py \\
      --cluster=<CLUSTER_NAME> \\
      --region=<REGION> \\
      -- \\
      --input_path=gs://<YOUR_BUCKET>/input/click_file.txt \\
      --rdd_output=gs://<YOUR_BUCKET>/output/rdd_output \\
      --df_output=gs://<YOUR_BUCKET>/output/df_output

─────────────────────────────────────────────────────────────────────────────
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ═══════════════════════════════════════════════════════════════════════════
#  UTILITY
# ═══════════════════════════════════════════════════════════════════════════

# Ordered list of bins used for final sorted display
BIN_ORDER = [
    "00-06 (Night)",
    "06-12 (Morning)",
    "12-18 (Afternoon)",
    "18-24 (Evening)",
]


def hour_to_bin(hour: int) -> str:
    """Return the time-of-day bin label for a given integer hour (0-23)."""
    if 0 <= hour < 6:
        return "00-06 (Night)"
    elif 6 <= hour < 12:
        return "06-12 (Morning)"
    elif 12 <= hour < 18:
        return "12-18 (Afternoon)"
    else:
        return "18-24 (Evening)"


def parse_hour_from_string(timestamp_str: str):
    """
    Extract the integer hour from a timestamp string.

    Handles common formats:
        "2024-01-15 14:30:45"
        "2024-01-15T14:30:45"
        "2024-01-15 14:30:45.000"
        "14:30:45"
    Returns None if the string cannot be parsed.
    """
    try:
        ts = timestamp_str.strip().replace("T", " ")

        # If there is a space → datetime, take the time part; else pure time
        if " " in ts:
            time_part = ts.split(" ")[1]
        else:
            time_part = ts

        hour = int(time_part.split(":")[0])
        if 0 <= hour <= 23:
            return hour
        return None
    except Exception:
        return None


def print_banner(title: str):
    width = 60
    print("\n" + "═" * width)
    print(f"  {title}")
    print("═" * width)


# ═══════════════════════════════════════════════════════════════════════════
#  APPROACH 1 — RDD
# ═══════════════════════════════════════════════════════════════════════════

def run_rdd_approach(sc, input_path: str, output_path: str):
    """
    Compute click counts by time-of-day bin using the RDD API.

    Pipeline:
        textFile() ──▶ filter header ──▶ map(parse & bin) ──▶
        filter(None) ──▶ reduceByKey(+) ──▶ sortByKey ──▶ save
    """
    print_banner("APPROACH 1 : RDD-based")

    # ── Step 1 : Load raw text ────────────────────────────────────────────
    raw_rdd = sc.textFile(input_path)

    # ── Step 2 : Identify header & detect timestamp column index ──────────
    header      = raw_rdd.first()
    header_cols = [c.strip().lower() for c in header.split(",")]
    ts_index    = header_cols.index("timestamp") if "timestamp" in header_cols else 1

    print(f"  Header columns : {header_cols}")
    print(f"  Timestamp index: {ts_index}")

    # ── Step 3 : Remove header & blank lines ──────────────────────────────
    data_rdd = raw_rdd.filter(
        lambda line: line.strip() != "" and line != header
    )

    # ── Step 4 : Parse each line → (bin_label, 1) ─────────────────────────
    def parse_line(line):
        parts = line.split(",")
        if len(parts) <= ts_index:
            return None
        hour = parse_hour_from_string(parts[ts_index])
        if hour is None:
            return None
        return (hour_to_bin(hour), 1)

    pairs_rdd = (
        data_rdd
        .map(parse_line)
        .filter(lambda x: x is not None)
    )

    # ── Step 5 : Aggregate — classic word-count pattern ───────────────────
    counts_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)

    # ── Step 6 : Sort by bin label (alphabetical = chronological) ─────────
    sorted_rdd = counts_rdd.sortByKey()

    # ── Step 7 : Collect & print results to driver ────────────────────────
    results = sorted_rdd.collect()
    print(f"\n  {'Time Bin':<25} {'Click Count':>12}")
    print("  " + "-" * 38)
    total = 0
    for bin_label, count in results:
        print(f"  {bin_label:<25} {count:>12,}")
        total += count
    print("  " + "-" * 38)
    print(f"  {'TOTAL':<25} {total:>12,}")

    # ── Step 8 : Write output to GCS as JSON ──────────────────────────────
    from pyspark.sql import SparkSession
    spark   = SparkSession.builder.getOrCreate()
    out_df  = spark.createDataFrame(sorted_rdd, schema=["time_bin", "click_count"])

    out_df.write.mode("overwrite").format("json").save(output_path)
    print(f"\n  >>> RDD output written to : {output_path}\n")

    return results


# ═══════════════════════════════════════════════════════════════════════════
#  APPROACH 2 — DataFrame API  (NO SparkSQL)
# ═══════════════════════════════════════════════════════════════════════════

def run_dataframe_approach(spark, input_path: str, output_path: str):
    """
    Compute click counts by time-of-day bin using the DataFrame API.

    Pipeline:
        read.csv() ──▶ cast timestamp ──▶ hour() ──▶
        when/otherwise (bin) ──▶ groupBy.count() ──▶ orderBy ──▶ save
    """
    print_banner("APPROACH 2 : DataFrame API-based")

    # ── Step 1 : Read CSV from GCS ────────────────────────────────────────
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    print("  Schema (raw):")
    df_raw.printSchema()
    print(f"  Total rows (raw): {df_raw.count():,}")

    # ── Step 2 : Normalise column names ───────────────────────────────────
    df = df_raw.toDF(*[c.strip().lower().replace(" ", "_")
                       for c in df_raw.columns])

    # ── Step 3 : Parse timestamp → extract hour ───────────────────────────
    #   coalesce tries multiple formats; returns NULL if none match.
    df = df.withColumn(
        "ts_parsed",
        F.coalesce(
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp(F.col("timestamp"), "MM/dd/yyyy HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "dd-MM-yyyy HH:mm:ss"),
        )
    )

    # Drop rows with unparseable timestamps
    df_clean = df.filter(F.col("ts_parsed").isNotNull())
    bad_rows = df.count() - df_clean.count()
    if bad_rows > 0:
        print(f"  WARNING : {bad_rows:,} rows dropped (unparseable timestamp).")

    # ── Step 4 : Extract hour (integer 0-23) ──────────────────────────────
    df_hr = df_clean.withColumn("hour", F.hour(F.col("ts_parsed")))

    # ── Step 5 : Assign bin via when / otherwise  (NO SparkSQL) ──────────
    df_binned = df_hr.withColumn(
        "time_bin",
        F.when((F.col("hour") >= 0)  & (F.col("hour") < 6),  "00-06 (Night)")
         .when((F.col("hour") >= 6)  & (F.col("hour") < 12), "06-12 (Morning)")
         .when((F.col("hour") >= 12) & (F.col("hour") < 18), "12-18 (Afternoon)")
         .otherwise("18-24 (Evening)")
    )

    # ── Step 6 : Aggregate — groupBy + count (DataFrame API) ─────────────
    df_counts = (
        df_binned
        .groupBy("time_bin")
        .count()
        .withColumnRenamed("count", "click_count")
        .orderBy("time_bin")
    )

    # ── Step 7 : Display results ──────────────────────────────────────────
    print("\n  Results (DataFrame API):")
    df_counts.show(truncate=False)

    # ── Step 8 : Write output to GCS as JSON ──────────────────────────────
    df_counts.write.mode("overwrite").format("json").save(output_path)
    print(f"  >>> DataFrame output written to : {output_path}\n")

    return df_counts


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN DRIVER
# ═══════════════════════════════════════════════════════════════════════════

def main(input_path: str, rdd_output: str, df_output: str):

    # ── Create a single SparkSession shared by both approaches ────────────
    spark = (
        SparkSession.builder
        .appName("ClickCount_Combined_Week4_GA3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    print("\n" + "█" * 65)
    print("  IIT Madras IBD — Week 4 GA3 : Click Count by Time Bins")
    print("█" * 65)
    print(f"  Input  : {input_path}")
    print(f"  Out1   : {rdd_output}  (RDD approach)")
    print(f"  Out2   : {df_output}  (DataFrame approach)")
    print("█" * 65 + "\n")

    # ── Run Approach 1 (RDD) ──────────────────────────────────────────────
    rdd_results = run_rdd_approach(sc, input_path, rdd_output)

    # ── Run Approach 2 (DataFrame) ────────────────────────────────────────
    df_results = run_dataframe_approach(spark, input_path, df_output)

    # ── Final side-by-side comparison ────────────────────────────────────
    print_banner("FINAL COMPARISON — RDD vs DataFrame")
    rdd_dict = dict(rdd_results)
    df_rows  = df_results.collect()

    print(f"\n  {'Time Bin':<25} {'RDD Count':>12}  {'DF Count':>12}")
    print("  " + "-" * 52)
    for bin_label in BIN_ORDER:
        rdd_val = rdd_dict.get(bin_label, 0)
        df_val  = next((r["click_count"] for r in df_rows
                        if r["time_bin"] == bin_label), 0)
        match   = "✓" if rdd_val == df_val else "✗ MISMATCH"
        print(f"  {bin_label:<25} {rdd_val:>12,}  {df_val:>12,}   {match}")
    print("  " + "-" * 52)
    print("\n  Both approaches must produce identical counts.\n")

    spark.stop()
    print("  Spark session stopped. Job complete.\n")


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IBD Week 4 GA3 — Click Count by Time-of-Day Bins "
                    "(RDD + DataFrame, no SparkSQL)"
    )
    parser.add_argument(
        "--input_path",
        required=True,
        help="GCS path to input file, e.g. gs://my-bucket/input/click_data.txt"
    )
    parser.add_argument(
        "--rdd_output",
        required=True,
        help="GCS path for RDD output, e.g. gs://my-bucket/output/rdd_output"
    )
    parser.add_argument(
        "--df_output",
        required=True,
        help="GCS path for DataFrame output, e.g. gs://my-bucket/output/df_output"
    )

    args = parser.parse_args()
    main(args.input_path, args.rdd_output, args.df_output)
