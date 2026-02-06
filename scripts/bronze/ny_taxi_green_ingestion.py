import argparse
import hashlib
from datetime import datetime

import apache_beam as beam
from apache_beam.io import parquetio
from apache_beam.options.pipeline_options import PipelineOptions

# BigQuery Bronze Schema
BQ_SCHEMA = """
trip_id:STRING,
VendorID:INTEGER,
lpep_pickup_datetime:TIMESTAMP,
lpep_dropoff_datetime:TIMESTAMP,
store_and_fwd_flag:STRING,
RatecodeID:FLOAT,
PULocationID:INTEGER,
DOLocationID:INTEGER,
passenger_count:FLOAT,
trip_distance:FLOAT,
fare_amount:FLOAT,
extra:FLOAT,
mta_tax:FLOAT,
tip_amount:FLOAT,
tolls_amount:FLOAT,
ehail_fee:FLOAT,
improvement_surcharge:FLOAT,
total_amount:FLOAT,
payment_type:FLOAT,
trip_type:FLOAT,
congestion_surcharge:FLOAT,
cbd_congestion_fee:FLOAT,
ingestion_ts:TIMESTAMP
"""

PARQUET_COLUMNS = [
    "VendorID",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "store_and_fwd_flag",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "trip_type",
    "congestion_surcharge",
    "cbd_congestion_fee",
]

# Helpers (Bronze-safe)
def safe_int(value):
    try:
        return int(value)
    except Exception:
        return None

def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None

def safe_datetime(value):
    try:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
    except Exception:
        return None

# Record parsing (physical validity only)
def parse_record(record):
    pickup_ts = safe_datetime(record.get("lpep_pickup_datetime"))
    dropoff_ts = safe_datetime(record.get("lpep_dropoff_datetime"))

    if pickup_ts is None or dropoff_ts is None:
        return None
    if pickup_ts > dropoff_ts:
        return None

    return {
        "VendorID": safe_int(record.get("VendorID")),
        "lpep_pickup_datetime": pickup_ts.isoformat(),
        "lpep_dropoff_datetime": dropoff_ts.isoformat(),
        "store_and_fwd_flag": record.get("store_and_fwd_flag") or "Unknown",
        "RatecodeID": safe_float(record.get("RatecodeID")),
        "PULocationID": safe_int(record.get("PULocationID")),
        "DOLocationID": safe_int(record.get("DOLocationID")),
        "passenger_count": safe_float(record.get("passenger_count")),
        "trip_distance": safe_float(record.get("trip_distance")),
        "fare_amount": safe_float(record.get("fare_amount")),
        "extra": safe_float(record.get("extra")),
        "mta_tax": safe_float(record.get("mta_tax")),
        "tip_amount": safe_float(record.get("tip_amount")),
        "tolls_amount": safe_float(record.get("tolls_amount")),
        "ehail_fee": safe_float(record.get("ehail_fee")),
        "improvement_surcharge": safe_float(record.get("improvement_surcharge")),
        "total_amount": safe_float(record.get("total_amount")),
        "payment_type": safe_float(record.get("payment_type")),
        "trip_type": safe_float(record.get("trip_type")),
        "congestion_surcharge": safe_float(record.get("congestion_surcharge")),
        "cbd_congestion_fee": safe_float(record.get("cbd_congestion_fee")),
    }

# Trip ID (deterministic technical surrogate)
def generate_trip_id(record):
    key = (
        f"{record.get('VendorID')}-"
        f"{record.get('lpep_pickup_datetime')}-"
        f"{record.get('lpep_dropoff_datetime')}-"
        f"{record.get('PULocationID')}-"
        f"{record.get('DOLocationID')}"
    )
    record["trip_id"] = hashlib.md5(key.encode("utf-8")).hexdigest()
    return record

def add_ingestion_ts(record):
    record["ingestion_ts"] = datetime.utcnow().isoformat() + "Z"
    return record

# Args
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--input_parquet", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--job_name", default="nyc-green-taxi-bronze-load")
    return parser.parse_known_args()

# Main
if __name__ == "__main__":
    args, pipeline_args = parse_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        runner="DirectRunner",   # DirectRunner for local test
        project=args.project,
        region=args.region,
        job_name=args.job_name,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
    )

    with beam.Pipeline(options=pipeline_options) as p:

        records = (
            p
            | "Read Parquet" >> parquetio.ReadFromParquet(
                file_pattern=args.input_parquet,
                columns=PARQUET_COLUMNS,
            )
            | "Parse Record" >> beam.Map(parse_record)
            | "Drop Invalid Records" >> beam.Filter(lambda x: x is not None)
            | "Generate Trip ID" >> beam.Map(generate_trip_id)
            | "Add Ingestion Timestamp" >> beam.Map(add_ingestion_ts)
        )

        records | "Write to BigQuery (Bronze)" >> beam.io.WriteToBigQuery(
            table=args.output_table,
            schema=BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            custom_gcs_temp_location=args.temp_location,
        )
