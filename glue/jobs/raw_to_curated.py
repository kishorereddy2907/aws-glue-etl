import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType

# ------------------------
# Job arguments
# ------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_database",
        "raw_table",
        "curated_s3_path"
    ]
)

# ------------------------
# Spark / Glue setup
# ------------------------
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------
# Read from Glue Catalog (RAW)
# ------------------------
df = glue_context.create_dynamic_frame.from_catalog(
    database=args["raw_database"],
    table_name=args["raw_table"]
).toDF()

# ------------------------
# Required columns check
# ------------------------
required_columns = [
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
]

missing = [c for c in required_columns if c not in df.columns]
if missing:
    raise Exception(f"Missing required columns: {missing}")

# ------------------------
# Cleansing
# ------------------------
df = (
    df
    .withColumn("player_name", trim(col("player_name")))
    .withColumn("opponent_name", trim(col("opponent_name")))
    .withColumn("format", trim(col("format")))
    .withColumn("year", col("year").cast(IntegerType()))
)

# ------------------------
# Deduplication
# ------------------------
df = df.dropDuplicates(["id"])

# ------------------------
# Column order (CRITICAL)
# ------------------------
df = df.select(
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
)

# ------------------------
# Write Parquet (NO partitions)
# ------------------------
(
    df.write
    .mode("append")
    .parquet(args["curated_s3_path"])
)

job.commit()
