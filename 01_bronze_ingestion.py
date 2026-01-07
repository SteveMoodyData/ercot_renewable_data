# Databricks notebook source
# MAGIC %md
# MAGIC # ERCOT Renewable Data Ingestion - Bronze Layer
# MAGIC 
# MAGIC This notebook ingests solar and wind forecast/actual data from ERCOT using the `gridstatus` library.
# MAGIC 
# MAGIC ## Data Sources
# MAGIC - Hourly Solar Actual & Forecast (system-wide and by region)
# MAGIC - Hourly Wind Actual & Forecast (system-wide and by region)
# MAGIC - 7-Day Load Forecast
# MAGIC - Fuel Mix (for context)
# MAGIC 
# MAGIC ## Architecture
# MAGIC - **Bronze**: Raw API responses stored as Delta tables with metadata
# MAGIC - **Silver**: Cleaned, standardized, and joined datasets
# MAGIC - **Gold**: Forecast accuracy metrics, ML features

# COMMAND ----------

# MAGIC %pip install gridstatus python-dotenv --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_timestamp, 
    year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema configuration
CATALOG = "ercot_energy"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Storage paths (adjust for your environment)
BRONZE_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/raw"

# API credentials from Databricks secrets
# Set these up: databricks secrets create-scope ercot
# databricks secrets put-secret ercot api_username
# databricks secrets put-secret ercot api_password  
# databricks secrets put-secret ercot subscription_key

def get_ercot_credentials():
    """Retrieve ERCOT API credentials from Databricks secrets."""
    return {
        "username": dbutils.secrets.get(scope="ercot", key="api_username"),
        "password": dbutils.secrets.get(scope="ercot", key="api_password"),
        "subscription_key": dbutils.secrets.get(scope="ercot", key="subscription_key"),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize gridstatus ErcotAPI

# COMMAND ----------

from gridstatus.ercot_api.ercot_api import ErcotAPI

def get_ercot_api() -> ErcotAPI:
    """Initialize authenticated ERCOT API client."""
    creds = get_ercot_credentials()
    
    return ErcotAPI(
        username=creds["username"],
        password=creds["password"],
        subscription_key=creds["subscription_key"],
        sleep_seconds=0.3,  # Rate limiting
        max_retries=3,
    )

# Test connection
api = get_ercot_api()
print("✓ ERCOT API connection established")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Source Definitions
# MAGIC 
# MAGIC Define the endpoints and metadata for each data source we want to ingest.

# COMMAND ----------

# Data source configuration - metadata-driven approach
DATA_SOURCES = {
    "solar_hourly": {
        "description": "Hourly solar actual and forecast - system wide",
        "method": "get_solar_actual_and_forecast_hourly",
        "endpoint": "/np4-737-cd/spp_hrly_avrg_actl_fcast",
        "frequency": "hourly",
        "table_name": "solar_hourly_actual_forecast",
        "partition_cols": ["year", "month", "day"],
    },
    "solar_hourly_regional": {
        "description": "Hourly solar actual and forecast - by geographic region",
        "method": "get_solar_actual_and_forecast_by_geographical_region_hourly",
        "endpoint": "/np4-745-cd/spp_hrly_actual_fcast_geo",
        "frequency": "hourly",
        "table_name": "solar_hourly_actual_forecast_regional",
        "partition_cols": ["year", "month", "day"],
    },
    "wind_hourly": {
        "description": "Hourly wind actual and forecast - system wide",
        "method": "get_wind_actual_and_forecast_hourly",
        "endpoint": "/np4-732-cd/wpp_hrly_avrg_actl_fcast",
        "frequency": "hourly",
        "table_name": "wind_hourly_actual_forecast",
        "partition_cols": ["year", "month", "day"],
    },
    "wind_hourly_regional": {
        "description": "Hourly wind actual and forecast - by geographic region",
        "method": "get_wind_actual_and_forecast_by_geographical_region_hourly",
        "endpoint": "/np4-742-cd/wpp_hrly_actual_fcast_geo",
        "frequency": "hourly",
        "table_name": "wind_hourly_actual_forecast_regional",
        "partition_cols": ["year", "month", "day"],
    },
    "load_forecast": {
        "description": "7-day load forecast by weather zone",
        "method": "get_load_forecast",
        "endpoint": "/np3-565-cd/lf_by_model_weather_zone",
        "frequency": "daily",
        "table_name": "load_forecast_7day",
        "partition_cols": ["year", "month"],
    },
    "fuel_mix": {
        "description": "Generation by fuel type",
        "method": "get_fuel_mix",
        "endpoint": None,  # Uses web scraping, not API
        "frequency": "5min",
        "table_name": "fuel_mix",
        "partition_cols": ["year", "month", "day"],
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion Functions

# COMMAND ----------

from gridstatus import Ercot

def ingest_solar_hourly(
    api: ErcotAPI,
    start_date: str,
    end_date: Optional[str] = None,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Ingest hourly solar actual and forecast data.
    
    Returns DataFrame with columns:
    - Time, Interval Start, Interval End
    - ACTUAL_SYSTEM_WIDE (actual generation in MW)
    - STPPF_SYSTEM_WIDE (Short-Term PV Power Forecast)
    - PVGRPP_SYSTEM_WIDE (PV Generation Resource Production Potential)
    - COP_HSL_SYSTEM_WIDE (Current Operating Plan High Sustained Limit)
    """
    df = api.get_solar_actual_and_forecast_hourly(
        date=start_date,
        end=end_date,
        verbose=verbose
    )
    
    # Add ingestion metadata
    df["_ingested_at"] = datetime.utcnow()
    df["_source"] = "ercot_api"
    df["_endpoint"] = "/np4-737-cd/spp_hrly_avrg_actl_fcast"
    
    return df


def ingest_solar_hourly_regional(
    api: ErcotAPI,
    start_date: str,
    end_date: Optional[str] = None,
    verbose: bool = False
) -> pd.DataFrame:
    """Ingest hourly solar data by geographic region."""
    df = api.get_solar_actual_and_forecast_by_geographical_region_hourly(
        date=start_date,
        end=end_date,
        verbose=verbose
    )
    
    df["_ingested_at"] = datetime.utcnow()
    df["_source"] = "ercot_api"
    df["_endpoint"] = "/np4-745-cd/spp_hrly_actual_fcast_geo"
    
    return df


def ingest_wind_hourly(
    api: ErcotAPI,
    start_date: str,
    end_date: Optional[str] = None,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Ingest hourly wind actual and forecast data.
    
    Returns DataFrame with columns similar to solar:
    - ACTUAL_SYSTEM_WIDE, STWPF_SYSTEM_WIDE, WGRPP_SYSTEM_WIDE, COP_HSL_SYSTEM_WIDE
    """
    df = api.get_wind_actual_and_forecast_hourly(
        date=start_date,
        end=end_date,
        verbose=verbose
    )
    
    df["_ingested_at"] = datetime.utcnow()
    df["_source"] = "ercot_api"
    df["_endpoint"] = "/np4-732-cd/wpp_hrly_avrg_actl_fcast"
    
    return df


def ingest_wind_hourly_regional(
    api: ErcotAPI,
    start_date: str,
    end_date: Optional[str] = None,
    verbose: bool = False
) -> pd.DataFrame:
    """Ingest hourly wind data by geographic region."""
    df = api.get_wind_actual_and_forecast_by_geographical_region_hourly(
        date=start_date,
        end=end_date,
        verbose=verbose
    )
    
    df["_ingested_at"] = datetime.utcnow()
    df["_source"] = "ercot_api"
    df["_endpoint"] = "/np4-742-cd/wpp_hrly_actual_fcast_geo"
    
    return df


def ingest_fuel_mix(
    start_date: str = "today",
    verbose: bool = False
) -> pd.DataFrame:
    """
    Ingest fuel mix data (uses Ercot class, not ErcotAPI).
    
    Note: Historical data not supported via this method.
    Use for real-time/today only.
    """
    ercot = Ercot()
    df = ercot.get_fuel_mix(date=start_date, verbose=verbose)
    
    df["_ingested_at"] = datetime.utcnow()
    df["_source"] = "ercot_web"
    df["_endpoint"] = "fuel-mix.json"
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Tables

# COMMAND ----------

def write_to_bronze(
    df: pd.DataFrame,
    table_name: str,
    partition_cols: list,
    mode: str = "append"
) -> None:
    """
    Write pandas DataFrame to Bronze Delta table.
    
    Args:
        df: Source pandas DataFrame
        table_name: Target table name (without catalog/schema)
        partition_cols: Columns to partition by
        mode: Write mode ('append' or 'overwrite')
    """
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # Add partition columns if time-based
    if "Interval Start" in spark_df.columns:
        spark_df = (
            spark_df
            .withColumn("year", year(col("Interval Start")))
            .withColumn("month", month(col("Interval Start")))
            .withColumn("day", dayofmonth(col("Interval Start")))
        )
    elif "Time" in spark_df.columns:
        spark_df = (
            spark_df
            .withColumn("year", year(col("Time")))
            .withColumn("month", month(col("Time")))
            .withColumn("day", dayofmonth(col("Time")))
        )
    
    # Full table path
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    
    # Write to Delta
    (
        spark_df
        .write
        .format("delta")
        .mode(mode)
        .partitionBy(*partition_cols)
        .option("mergeSchema", "true")
        .saveAsTable(full_table_name)
    )
    
    print(f"✓ Wrote {len(df):,} rows to {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load Logic

# COMMAND ----------

def get_last_loaded_date(table_name: str) -> Optional[datetime]:
    """Get the most recent date loaded for incremental processing."""
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    
    try:
        result = spark.sql(f"""
            SELECT MAX(DATE(`Interval Start`)) as max_date 
            FROM {full_table_name}
        """).collect()
        
        if result and result[0]["max_date"]:
            return result[0]["max_date"]
    except Exception as e:
        print(f"Table {full_table_name} not found or empty: {e}")
    
    return None


def determine_load_dates(
    table_name: str,
    lookback_days: int = 7,
    default_start: str = "2024-01-01"
) -> tuple[str, str]:
    """
    Determine start and end dates for incremental load.
    
    Returns:
        Tuple of (start_date, end_date) as strings
    """
    last_loaded = get_last_loaded_date(table_name)
    
    if last_loaded:
        # Start from day after last loaded, with lookback for late-arriving data
        start_date = (last_loaded - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    else:
        # Initial load
        start_date = default_start
    
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Load range: {start_date} to {end_date}")
    return start_date, end_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Bronze Ingestion

# COMMAND ----------

# Widget parameters for job scheduling
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("load_type", "incremental", ["incremental", "full", "date_range"])
dbutils.widgets.multiselect(
    "sources", 
    "solar_hourly,wind_hourly", 
    list(DATA_SOURCES.keys()),
    "Data Sources"
)

# COMMAND ----------

def run_bronze_ingestion():
    """Main ingestion orchestration."""
    
    load_type = dbutils.widgets.get("load_type")
    selected_sources = dbutils.widgets.get("sources").split(",")
    
    api = get_ercot_api()
    
    results = {}
    
    for source_key in selected_sources:
        if source_key not in DATA_SOURCES:
            print(f"⚠ Unknown source: {source_key}")
            continue
            
        source_config = DATA_SOURCES[source_key]
        table_name = source_config["table_name"]
        
        print(f"\n{'='*60}")
        print(f"Processing: {source_config['description']}")
        print(f"{'='*60}")
        
        try:
            # Determine date range
            if load_type == "date_range":
                start_date = dbutils.widgets.get("start_date")
                end_date = dbutils.widgets.get("end_date")
            elif load_type == "incremental":
                start_date, end_date = determine_load_dates(table_name)
            else:  # full
                start_date = "2024-01-01"
                end_date = datetime.now().strftime("%Y-%m-%d")
            
            # Ingest based on source type
            if source_key == "solar_hourly":
                df = ingest_solar_hourly(api, start_date, end_date, verbose=True)
            elif source_key == "solar_hourly_regional":
                df = ingest_solar_hourly_regional(api, start_date, end_date, verbose=True)
            elif source_key == "wind_hourly":
                df = ingest_wind_hourly(api, start_date, end_date, verbose=True)
            elif source_key == "wind_hourly_regional":
                df = ingest_wind_hourly_regional(api, start_date, end_date, verbose=True)
            elif source_key == "fuel_mix":
                df = ingest_fuel_mix(verbose=True)
            else:
                print(f"⚠ No ingestion function for {source_key}")
                continue
            
            # Write to bronze
            if not df.empty:
                write_to_bronze(
                    df=df,
                    table_name=table_name,
                    partition_cols=source_config["partition_cols"],
                    mode="append"
                )
                results[source_key] = {"status": "success", "rows": len(df)}
            else:
                print(f"⚠ No data returned for {source_key}")
                results[source_key] = {"status": "empty", "rows": 0}
                
        except Exception as e:
            print(f"✗ Error processing {source_key}: {e}")
            results[source_key] = {"status": "error", "error": str(e)}
    
    return results

# COMMAND ----------

# Run the ingestion
results = run_bronze_ingestion()

# Summary
print("\n" + "="*60)
print("INGESTION SUMMARY")
print("="*60)
for source, result in results.items():
    status = result.get("status", "unknown")
    rows = result.get("rows", 0)
    error = result.get("error", "")
    
    if status == "success":
        print(f"✓ {source}: {rows:,} rows")
    elif status == "empty":
        print(f"○ {source}: no data")
    else:
        print(f"✗ {source}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def run_bronze_quality_checks(table_name: str) -> dict:
    """Run basic quality checks on bronze table."""
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    
    checks = {}
    
    # Row count
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]["cnt"]
    checks["row_count"] = count
    
    # Date range
    date_range = spark.sql(f"""
        SELECT 
            MIN(DATE(`Interval Start`)) as min_date,
            MAX(DATE(`Interval Start`)) as max_date
        FROM {full_table_name}
    """).collect()[0]
    checks["min_date"] = str(date_range["min_date"])
    checks["max_date"] = str(date_range["max_date"])
    
    # Null check on key columns
    null_check = spark.sql(f"""
        SELECT 
            SUM(CASE WHEN `Interval Start` IS NULL THEN 1 ELSE 0 END) as null_interval_start,
            SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END) as null_ingested_at
        FROM {full_table_name}
    """).collect()[0]
    checks["null_interval_start"] = null_check["null_interval_start"]
    checks["null_ingested_at"] = null_check["null_ingested_at"]
    
    # Duplicate check
    dup_check = spark.sql(f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT `Interval Start`, COUNT(*) as cnt
            FROM {full_table_name}
            GROUP BY `Interval Start`
            HAVING COUNT(*) > 1
        )
    """).collect()[0]
    checks["duplicate_intervals"] = dup_check["dup_count"]
    
    return checks

# COMMAND ----------

# Run quality checks on loaded tables
for source_key in dbutils.widgets.get("sources").split(","):
    if source_key in DATA_SOURCES:
        table_name = DATA_SOURCES[source_key]["table_name"]
        try:
            checks = run_bronze_quality_checks(table_name)
            print(f"\nQuality checks for {table_name}:")
            for check, value in checks.items():
                print(f"  {check}: {value}")
        except Exception as e:
            print(f"  Could not run checks: {e}")
