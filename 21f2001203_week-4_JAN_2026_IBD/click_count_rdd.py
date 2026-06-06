"""
Week 4 - Graded Assignment 3
=============================
PySpark Program: Click Count by Time-of-Day Bins
Implementation : RDD-based approach

Author  : (Your Name / Roll Number)
Date    : 2026-03-08

Description:
    Reads a CSV file (user_id, timestamp) from Google Cloud Storage,
    buckets each click into one of four 6-hour time windows using RDDs,
    and writes the aggregated counts back to GCS as a JSON file.

Time bins:
    0  – 6  hours  →  "00-06 (Night)"
    6  – 12 hours  →  "06-12 (Morning)"
    12 – 18 hours  →  "12-18 (Afternoon)"
    18 – 24 hours  →  "18-24 (Evening)"

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark gs://<BUCKET>/scripts/click_count_rdd.py \
        --cluster=<CLUSTER_NAME> \
        --region=<REGION> \
        -- \
        --input_path=gs://<BUCKET>/input/click_file.txt \
        --output_path=gs://<BUCKET>/output/rdd_output
"""

import sys
import argparse
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Helper: map a timestamp string → 6-hour bin label
# Supported formats:
#   "2024-01-15 14:30:45"
#   "2024-01-15T14:30:45"
#   "2024-01-15 14:30:45.000"
#   "14:30:45"   (time-only)
# ---------------------------------------------------------------------------
def get_time_bin(timestamp_str):
    """Extract the hour from a timestamp string and return the bin label."""
    try:
        ts = timestamp_str.strip()

        # Handle both space and 'T' separator  (ISO 8601 & common CSV formats)
        if "T" in ts:
            ts = ts.replace("T", " ")

        # Extract the time portion
        if " " in ts:
            time_part = ts.split(" ")[1]   # "14:30:45" or "14:30:45.000"
        else:
            time_part = ts                  # already "14:30:45"

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
        return None                          # malformed rows → filtered out


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def main(input_path, output_path):

    # Use SparkSession (same pattern as course notebooks) so Dataproc/YARN
    # does not raise "Cannot run multiple SparkContexts" errors.
    spark = (
        SparkSession.builder
        .appName("ClickCount_RDD")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # ── 1. Read raw text from GCS ─────────────────────────────────────────
    raw_rdd = sc.textFile(input_path)

    # ── 2. Identify & skip the header line ────────────────────────────────
    header = raw_rdd.first()                 # "user_id,timestamp"
    data_rdd = raw_rdd.filter(lambda line: line != header and line.strip() != "")

    # ── 3. Parse lines, detect column positions dynamically ───────────────
    #       Column order: figure out which index holds the timestamp
    header_cols = [c.strip().lower() for c in header.split(",")]
    try:
        ts_index = header_cols.index("timestamp")
    except ValueError:
        ts_index = 1                         # default: 2nd column

    print(f">>> Header  : {header}")
    print(f">>> ts_index: {ts_index}")

    # ── 4. Parse each row → (bin_label, 1) ────────────────────────────────
    def parse_and_bin(line):
        parts = line.split(",")
        if len(parts) <= ts_index:
            return None
        ts_val  = parts[ts_index].strip()
        bin_lbl = get_time_bin(ts_val)
        if bin_lbl is None:
            return None
        return (bin_lbl, 1)

    parsed_rdd = (
        data_rdd
        .map(parse_and_bin)
        .filter(lambda x: x is not None)    # drop malformed rows
    )

    # ── 5. Count clicks per bin using reduceByKey ─────────────────────────
    counts_rdd = parsed_rdd.reduceByKey(lambda a, b: a + b)

    # ── 6. Sort by bin label (alphabetical → chronological order) ─────────
    sorted_rdd = counts_rdd.sortByKey()

    # ── 7. Print results to Driver log ────────────────────────────────────
    print("\n" + "=" * 50)
    print("  Click Count by Time-of-Day Bin  (RDD)")
    print("=" * 50)
    results = sorted_rdd.collect()
    for bin_label, count in results:
        print(f"  {bin_label:<25}  :  {count:>8,} clicks")
    print("=" * 50 + "\n")

    # ── 8. Convert to a DF-compatible format and save as JSON to GCS ──────
    output_df = spark.createDataFrame(
        sorted_rdd,
        schema=["time_bin", "click_count"]
    )
    output_df.show(truncate=False)

    output_df.write \
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
        description="Click count by time-of-day bins — RDD approach"
    )
    parser.add_argument(
        "--input_path",
        required=True,
        help="GCS path to input file, e.g. gs://bucket/input/click_file.txt"
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="GCS path for output JSON, e.g. gs://bucket/output/rdd_output"
    )

    args = parser.parse_args()
    main(args.input_path, args.output_path)
