# Run config_file

from config.adls_auth import configure_adls

configure_adls(spark)

# ------------------------------------------------------------------------------------------------------------------

# Started logging
import logging
from pyspark.sql.functions import current_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("youtube_pipeline")

PIPELINE_STAGE = "SILVER"  # change per notebook

logger.info(f"{PIPELINE_STAGE} stage started")

# ------------------------------------------------------------------------------------------------------------------

# Reading from Bronze layer and recreating the dataframe in silver layer to augment it with Video Views, likes and comments data

from pyspark.sql.functions import col, explode, current_timestamp

bronze_base_path = "abfss://bronze@DATALAKE_NAME.dfs.core.windows.net/youtube/raw/*/*/*.json"

# Read all JSON files directly from Bronze
parsed_df = spark.read.json(bronze_base_path)

# Explode items array
videos_df = parsed_df.select(explode(col("items")).alias("video"))

# Extract fields
silver_df = videos_df.select(
    col("video.id.videoId").alias("video_id"),
    col("video.snippet.title").alias("title"),
    col("video.snippet.channelTitle").alias("channel_title"),
    col("video.snippet.publishedAt").alias("published_at")
)

# Add ingestion timestamp
silver_df = silver_df.withColumn("ingestion_time", current_timestamp())

# Remove duplicates
silver_df = silver_df.dropDuplicates(["video_id"])

silver_df.display()

# ------------------------------------------------------------------------------------------------------------------

# Ingesting views,likes and comments data from Youtube API videos end point

from pyspark.sql.functions import col, explode
from pyspark.sql.types import LongType
import json
import requests

# Extract video IDs safely
video_ids = [row.video_id for row in silver_df.select("video_id").collect() if row.video_id is not None]

if len(video_ids) == 0:
    print("No video IDs found")
else:
    stats_url = "https://www.googleapis.com/youtube/v3/videos"

    stats_params = {
        "part": "statistics",
        "id": ",".join(video_ids),
        "key":'YOTUBE_API_KEY'
    }

    stats_response = requests.get(stats_url, params=stats_params)

    stats_data = stats_response.json()

    # Convert JSON to Spark DataFrame
    stats_df = spark.read.json(
        spark.sparkContext.parallelize([json.dumps(stats_data)])
    )

    # Explode items array
    stats_exploded = stats_df.select(explode(col("items")).alias("video_stats"))

    # Extract and cast statistics
    stats_clean = stats_exploded.select(
        col("video_stats.id").alias("video_id"),
        col("video_stats.statistics.viewCount").cast(LongType()).alias("view_count"),
        col("video_stats.statistics.likeCount").cast(LongType()).alias("like_count"),
        col("video_stats.statistics.commentCount").cast(LongType()).alias("comment_count")
    )

    stats_clean.display()
# ------------------------------------------------------------------------------------------------------------------

# Join Video_df and stats_df
final_silver_df = silver_df.join(stats_clean, on="video_id", how="left")

# ------------------------------------------------------------------------------------------------------------------

# Data Quality checks

logger.info("Starting Silver transformation")

total_records = final_silver_df.count()

if total_records == 0:
    logger.error("Silver dataset empty")
    raise Exception("Silver DQ failed")

null_ids = final_silver_df.filter(col("video_id").isNull()).count()

if null_ids > 0:
    raise Exception("Silver DQ failed: NULL video_id")

duplicates = final_silver_df.groupBy("video_id").count().filter(col("count") > 1).count()

if duplicates > 0:
    raise Exception("Silver DQ failed: duplicate video_id")

logger.info(f"Silver DQ passed: {total_records} records")

# ------------------------------------------------------------------------------------------------------------------

#Storing Silver_df into delta tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

silver_path = "abfss://silver@DATALAKE_NAME.dfs.core.windows.net/youtube/clean_delta/"

try:
    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("target").merge(
        final_silver_df.alias("source"),
        "target.video_id = source.video_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

except AnalysisException:
    final_silver_df.write.format("delta").mode("overwrite").save(silver_path)

# ------------------------------------------------------------------------------------------------------------------

# Audit Entry
audit_path = "abfss://gold@DATALAKE_NAME.dfs.core.windows.net/youtube/audit"
audit_df = spark.createDataFrame([
    (PIPELINE_STAGE, "success", total_records)
], ["stage", "status", "record_count"]) \
.withColumn("run_time", current_timestamp())

audit_df.write.format("delta").mode("append").save(audit_path)