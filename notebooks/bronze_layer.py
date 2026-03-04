# Run config_file

from config.adls_auth import configure_adls

configure_adls(spark)


# Logging started

import logging
from pyspark.sql.functions import current_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("youtube_pipeline")

PIPELINE_STAGE = "BRONZE"  # change per notebook

logger.info(f"{PIPELINE_STAGE} stage started")

# ------------------------------------------------------------------------------------------------------------------

# Ingesting from Youtube API

import requests
import json
from datetime import datetime
from pyspark.sql import Row

API_KEY = "<YOUTUBE_API_KEY>"

SEARCH_QUERY = "YYOUR_NICHE"

search_url = "https://www.googleapis.com/youtube/v3/search"

search_params = {
    "part": "snippet",
    "q": SEARCH_QUERY,
    "type": "video",
    "order": "date",
    "maxResults": 50,
    "key": API_KEY
}

search_response = requests.get(search_url, params=search_params)

search_data = search_response.json()

print("Videos fetched:", len(search_data['items']))

# ------------------------------------------------------------------------------------------------------------------

# Data quality check post API call

if "items" not in search_data or len(search_data["items"]) == 0:
    logger.error("No videos fetched from API")
    raise Exception("Bronze DQ failed")

logger.info(f"Bronze DQ passed: {len(search_data['items'])} videos fetched")

# ------------------------------------------------------------------------------------------------------------------

# Saving the files to ADLS Gen2 Bronze layer

bronze_df = spark.createDataFrame([Row(json_data=json.dumps(search_data))])

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

bronze_path = f"abfss://bronze@DATALAKE_NAME.dfs.core.windows.net/youtube/raw/{SEARCH_QUERY}/{timestamp}/"

    # Save raw JSON as text file

dbutils.fs.put(bronze_path + "data.json", json.dumps(search_data), overwrite=True)

print("Bronze data saved to:", bronze_path)

# ------------------------------------------------------------------------------------------------------------------


# Audit table entry

audit_path = "abfss://gold@DATALAKE_NAME.dfs.core.windows.net/youtube/audit"

audit_df = spark.createDataFrame([
    (PIPELINE_STAGE, "success", len(search_data["items"]))
], ["stage", "status", "record_count"]) \
.withColumn("run_time", current_timestamp())

audit_df.write.format("delta").mode("append").save(audit_path)

# ------------------------------------------------------------------------------------------------------------------

