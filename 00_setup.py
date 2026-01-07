# Databricks notebook source
# MAGIC %md
# MAGIC # ERCOT Energy Platform Setup
# MAGIC 
# MAGIC Run this notebook once to create the Unity Catalog structure for the ERCOT energy data platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "ercot_energy"
SCHEMAS = ["bronze", "silver", "gold"]

# Optional: External location for storage
# EXTERNAL_LOCATION = "abfss://ercot-data@yourstorageaccount.dfs.core.windows.net/"
EXTERNAL_LOCATION = None  # Use managed storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

print(f"✓ Catalog '{CATALOG}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

for schema in SCHEMAS:
    if EXTERNAL_LOCATION:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {schema}
            MANAGED LOCATION '{EXTERNAL_LOCATION}/{schema}'
        """)
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    print(f"✓ Schema '{CATALOG}.{schema}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volumes for Raw Files (Optional)

# COMMAND ----------

# Create volume for storing raw API responses if needed
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files
""")

print(f"✓ Volume '{CATALOG}.bronze.raw_files' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Secrets Scope
# MAGIC 
# MAGIC Run these commands in Databricks CLI to set up ERCOT API credentials:
# MAGIC 
# MAGIC ```bash
# MAGIC # Create scope
# MAGIC databricks secrets create-scope ercot
# MAGIC 
# MAGIC # Add credentials
# MAGIC databricks secrets put-secret ercot api_username --string-value "your_username"
# MAGIC databricks secrets put-secret ercot api_password --string-value "your_password"
# MAGIC databricks secrets put-secret ercot subscription_key --string-value "your_subscription_key"
# MAGIC ```
# MAGIC 
# MAGIC To get these credentials:
# MAGIC 1. Register at https://apiexplorer.ercot.com/
# MAGIC 2. Follow the authentication guide at https://developer.ercot.com/applications/pubapi/user-guide/registration-and-authentication/

# COMMAND ----------

# Verify secrets are accessible (won't print values)
try:
    _ = dbutils.secrets.get(scope="ercot", key="api_username")
    print("✓ ERCOT secrets scope configured")
except Exception as e:
    print(f"⚠ ERCOT secrets not configured. Run the CLI commands above.")
    print(f"  Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all schemas
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions (Optional)
# MAGIC 
# MAGIC Uncomment and modify these statements to grant access to other users/groups.

# COMMAND ----------

# # Grant data team access
# spark.sql(f"GRANT USAGE ON CATALOG {CATALOG} TO `data-team`")
# spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG}.gold TO `data-team`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `data-team`")

# # Grant analysts read access to gold only
# spark.sql(f"GRANT USAGE ON CATALOG {CATALOG} TO `analysts`")
# spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG}.gold TO `analysts`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `analysts`")

# COMMAND ----------

print("\n" + "="*60)
print("SETUP COMPLETE")
print("="*60)
print(f"""
Next steps:
1. Configure ERCOT API secrets (see CLI commands above)
2. Run 01_bronze_ingestion to load data
3. Run 02_silver_transformations to clean data
4. Run 03_gold_aggregations to create analytics tables

Catalog structure:
  {CATALOG}/
  ├── bronze/     # Raw API data
  │   ├── solar_hourly_actual_forecast
  │   ├── solar_hourly_actual_forecast_regional
  │   ├── wind_hourly_actual_forecast
  │   ├── wind_hourly_actual_forecast_regional
  │   └── fuel_mix
  ├── silver/     # Cleaned & standardized
  │   ├── solar_hourly
  │   ├── wind_hourly
  │   ├── solar_hourly_regional
  │   ├── wind_hourly_regional
  │   ├── renewable_generation_combined
  │   └── renewable_generation_totals
  └── gold/       # Analytics & ML ready
      ├── solar_forecast_accuracy_daily
      ├── solar_forecast_accuracy_by_hour
      ├── wind_forecast_accuracy_daily
      ├── wind_forecast_accuracy_by_hour
      ├── generation_hourly_patterns
      ├── generation_monthly_summary
      ├── generation_dow_patterns
      ├── curtailment_hourly_patterns
      ├── curtailment_daily_summary
      └── solar_forecast_features
""")
