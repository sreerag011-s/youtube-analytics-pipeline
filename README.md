# YouTube Data Engineering Pipeline

End-to-end YouTube analytics pipeline built using Azure Databricks, ADLS Gen2, Delta Lake, and Medallion Architecture.

**Project Overview:**

This project builds a scalable data pipeline to analyze YouTube content trends for a specific niche.
The pipeline collects data from the YouTube Data API, processes it using Spark, and generates insights such as:

1.)Best time to post content
2.)Fastest growing videos
3.)High engagement topics
4.)Competitor channel analysis

**Architecture**

YouTube API
     ↓
Bronze Layer (Raw ingestion)
     ↓
Silver Layer (Cleaned structured data)
     ↓
Gold Layer (Analytics insights)

----Architecture Diagram here----

**Tech Stack**

Component	Technology
Data Processing	Apache Spark
Compute	Azure Databricks
Storage	ADLS Gen2
Format	Delta Lake
Orchestration	Databricks Workflows
Language	Python / PySpark
Data Source	YouTube Data API
<img width="386" height="195" alt="image" src="https://github.com/user-attachments/assets/4f13cd31-b065-4851-a69d-7e3d1b7b8434" />

**Data Quality Checks**

The pipeline includes built-in data quality validation:
  1.)Schema validation
  2.)Null checks
  3.)Duplicate detection
  4.)Range validation
  5.)Business rule validation

Monitoring includes:

  1.)Structured logging
  2.)Audit Delta table
  3.)Databricks Workflow monitoring
  4.)Email alerts for pipeline failures

**Pipeline Orchestration**

The pipeline is automated using Databricks Workflows, executing the notebooks sequentially:

  bronze_ingestion
       ↓
  silver_delta_merge
       ↓
  gold_analytics
  
Scheduled to run daily.

**Security**

This project avoids hard-coding secrets in code. Instead shows placeholders. For local runs this is fine.
But in a production environment, we should :

  1.)Azure Service Principal is used for authentication
  2.)Access is controlled using Azure RBAC
  3.)Secrets are stored in Azure Key Vault or environment variables
  4.)Databricks notebooks authenticate using OAuth

**Project Structure**

youtube-data-engineering-pipeline
│
├── notebooks
│   ├── youtube_bronze_ingestion.py
│   ├── youtube_silver_delta_merge.py
│   └── youtube_gold_analytics.py
│
├── config
│   └── adls_auth.py
│
├── docs
│   └── architecture_diagram.png
│
└── README.md

**Future Improvements**

  1.)Add streaming ingestion
  2.)Implement Great Expectations for data quality
  3.)Integrate Power BI dashboards
  4.)Use Azure Key Vault for secrets
  5.)Add CI/CD with GitHub Actions

**This pipeline can be used by content creators to identify optimal posting times, discover high-performing topics, and analyze competitor channels to improve YouTube growth strategy.**
