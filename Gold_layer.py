# Run config_file

from config.adls_auth import configure_adls

configure_adls(spark)

#----------------------------------------------------------------------------------------------------

#Logging start
import logging
from pyspark.sql.functions import current_timestamp
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("youtube_pipeline")

PIPELINE_STAGE = "GOLD"

logger.info(f"{PIPELINE_STAGE} stage started")

#-----------------------------------------------------------------------------------------------------

#Read silver layer
from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_utc_timestamp,
    hour,
    dayofweek,
    datediff,
    current_date,
    avg,
    count,
    sum,
    desc,
    split,
    explode,
    expr
)

silver_path = "abfss://silver@DATALAKE_NAME.dfs.core.windows.net/youtube/clean_delta/"

silver_df = spark.read.format("delta").load(silver_path)

# Convert timestamps and normalize timezone (IST example)
silver_df = silver_df.withColumn(
    "publish_timestamp_utc",
    to_timestamp(col("published_at"))
)

silver_df = silver_df.withColumn(
    "publish_timestamp_local",
    from_utc_timestamp(col("publish_timestamp_utc"), "Asia/Kolkata")
)

# Add time features
silver_df = silver_df.withColumn(
    "publish_hour", hour(col("publish_timestamp_local"))
).withColumn(
    "publish_day", dayofweek(col("publish_timestamp_local"))
)

# Calculate video age
silver_df = silver_df.withColumn(
    "days_since_publish",
    datediff(current_date(), col("publish_timestamp_local"))
)

# Remove invalid rows
silver_df = silver_df.filter(col("days_since_publish") > 0)

# Calculate velocity metric
silver_df = silver_df.withColumn(
    "views_per_day",
    col("view_count") / col("days_since_publish")
)

# Calculate engagement metrics
silver_df = silver_df.withColumn(
    "like_rate",
    col("like_count") / col("view_count")
).withColumn(
    "comment_rate",
    col("comment_count") / col("view_count")
)
silver_df.display()

#-----------------------------------------------------------------------------------------------------

# Simple diagnostic to chekc if video_count < 3, if this is true Metric 1 - Best posting time will 
# have no rows and this is expected.

gold_posting_time_debug = silver_df.groupBy(
    "publish_day", "publish_hour"
).agg(
    avg("views_per_day").alias("avg_views_per_day"),
    count("*").alias("video_count")
).orderBy(col("avg_views_per_day").desc())

gold_posting_time_debug.display()

#------------------------------------------------------------------------------------------------------

# Analytical Metrics

# Metric 1
#Best posting time

gold_posting_time = silver_df.groupBy(
    "publish_day", "publish_hour"
).agg(
    avg("views_per_day").alias("avg_views_per_day"),
    avg("like_rate").alias("avg_like_rate"),
    avg("comment_rate").alias("avg_comment_rate"),
    count("*").alias("video_count")
).filter(
    col("video_count") >= 3
).orderBy(desc("avg_views_per_day"))
gold_posting_time.display()

# Metric 2
# Fastest growing videos (Viral detection)

gold_velocity = silver_df.select(
    "video_id",
    "title",
    "channel_title",
    "view_count",
    "days_since_publish",
    "views_per_day"
).orderBy(desc("views_per_day"))

gold_velocity.display()


# Metric 3
# Highest engagement videos
gold_engagement = silver_df.select(
    "video_id",
    "title",
    "channel_title",
    "view_count",
    "like_rate",
    "comment_rate"
).orderBy(desc("like_rate"))

gold_engagement.display()

# Metric 4
# gold_keywords = silver_df.select(
gold_keywords = silver_df.select(
    explode(split(col("title"), " ")).alias("keyword"),
    col("views_per_day")
).groupBy("keyword").agg(
    avg("views_per_day").alias("avg_views_per_day"),
    count("*").alias("keyword_count")
).filter(
    col("keyword_count") >= 3
).orderBy(desc("avg_views_per_day"))

gold_keywords.display()


# Metric 5
# Channel benchmarking
gold_channels = silver_df.groupBy("channel_title").agg(
    count("video_id").alias("total_videos"),
    avg("views_per_day").alias("avg_views_per_day"),
    avg("like_rate").alias("avg_like_rate")
).orderBy(desc("avg_views_per_day"))

gold_channels.display()

#------------------------------------------------------------------------------------------------------

# Writing to Gold_Layer Delata Tables

gold_path = "abfss://gold@DATALAKE_NAME.dfs.core.windows.net/youtube/"

gold_posting_time.write.format("delta").mode("overwrite").save(gold_path + "posting_time")

gold_velocity.write.format("delta").mode("overwrite").save(gold_path + "velocity")

gold_engagement.write.format("delta").mode("overwrite").save(gold_path + "engagement")

gold_keywords.write.format("delta").mode("overwrite").save(gold_path + "keywords")

gold_channels.write.format("delta").mode("overwrite").save(gold_path + "channels")

#-----------------------------------------------------------------------------------------------------

# Data Quality checks

if gold_velocity.count() == 0:
    raise Exception("Gold DQ failed: empty velocity table")

null_velocity = silver_df.filter(col("views_per_day").isNull()).count()

if null_velocity > 0:
    raise Exception("Gold DQ failed: null velocity")

logger.info("Gold DQ passed")

#-------------------------------------------------------------------------------------------------------

# Audit Entry

from pyspark.sql.functions import current_timestamp

audit_path = "abfss://gold@DATALAKE_NAME.dfs.core.windows.net/youtube/audit"

gold_record_count = silver_df.count()

audit_df = spark.createDataFrame([
    ("gold", "success", gold_record_count, "Gold analytics generated")
], ["pipeline_stage", "status", "record_count", "message"]) \
.withColumn("run_time", current_timestamp())

audit_df.write.format("delta").mode("append").save(audit_path)

print("Gold audit entry written")