import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# ---------------------------
# Job arguments
# ---------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_DB", "SOURCE_TABLE", "TARGET_S3_PATH"]
)

spark = SparkSession.builder.getOrCreate()
glue_context = GlueContext(spark.sparkContext)

# ---------------------------
# Read from Glue Catalog
# ---------------------------
df = glue_context.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DB"],
    table_name=args["SOURCE_TABLE"]
).toDF()

# ---------------------------
# Basic validation
# ---------------------------
required_cols = ["id", "date", "format"]

for c in required_cols:
    df = df.filter(col(c).isNotNull())

df = df.filter(col("format").isin("Blitz", "Rapid", "Classical"))

# ---------------------------
# Normalization
# ---------------------------
df = (
    df.withColumn("player_name", lower(col("player_name")))
      .withColumn("opponent_name", lower(col("opponent_name")))
      .withColumn("rating_diff", col("player_rating") - col("opponent_rating"))
)

# ---------------------------
# Deduplication
# ---------------------------
df = df.dropDuplicates(["id"])

# ---------------------------
# Write curated Parquet
# ---------------------------
(
    df.write
      .mode("overwrite")
      .partitionBy("year", "format")
      .parquet(args["TARGET_S3_PATH"])
)
