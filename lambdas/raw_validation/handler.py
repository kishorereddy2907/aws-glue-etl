import csv
import boto3
from io import TextIOWrapper

s3 = boto3.client("s3")

REQUIRED_COLUMNS = {
    "id",
    "player_name",
    "opponent_name",
    "player_rating",
    "opponent_rating",
    "format",
    "date",
    "year",
    "result",
    "player_color",
    "opponent_color",
    "result_raw",
    "moves"
}

def lambda_handler(event, context):
    bucket = event["bucket"]
    key = event["key"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    reader = csv.DictReader(TextIOWrapper(obj["Body"], encoding="utf-8"))

    rows = list(reader)

    # 1. Row count check
    if not rows:
        raise Exception("RAW file has zero rows")

    # 2. Required columns
    actual_columns = set(reader.fieldnames)
    missing = REQUIRED_COLUMNS - actual_columns
    extra = actual_columns - REQUIRED_COLUMNS

    if missing:
        raise Exception(f"Missing columns: {missing}")

    if extra:
        raise Exception(f"Unexpected columns (schema drift): {extra}")

    # 3. Duplicate ID check
    ids = [r["id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise Exception("Duplicate IDs detected")

    return {
        "status": "PASS",
        "row_count": len(rows)
    }
