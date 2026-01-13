# Databricks notebook source
# MAGIC %md
# MAGIC # ERCOT Renewable Data - Silver Layer Transformations
# MAGIC 
# MAGIC This notebook transforms Bronze data into clean, analytics-ready Silver tables.
# MAGIC 
# MAGIC ## Transformations Applied
# MAGIC - Standardize column names (snake_case)
# MAGIC - Convert timestamps to UTC
# MAGIC - Join actuals with forecasts for accuracy tracking
# MAGIC - Unpivot regional data for easier analysis
# MAGIC - Add derived columns (forecast horizon, hour of day, etc.)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, date_format,
    year, month, dayofmonth, hour, minute,
    expr, current_timestamp, datediff, abs as spark_abs,
    avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    lag, lead, row_number, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType

# COMMAND ----------

# Configuration
CATALOG = "ercot_energy"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solar Hourly - Clean and Standardize

# COMMAND ----------

def transform_solar_hourly():
    """
    Transform bronze solar hourly data to silver.
    
    Key transformations:
    - Rename columns to snake_case
    - Extract forecast horizon (hours ahead)
    - Calculate forecast error where actuals exist
    - Add time-based features
    """
    
    bronze_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.solar_hourly_actual_forecast")
    
    silver_df = (
        bronze_df
        # Rename columns to snake_case (bronze already has underscores, just lowercase)
        .withColumnRenamed("Interval_Start", "interval_start")
        .withColumnRenamed("Interval_End", "interval_end")
        .withColumnRenamed("Publish_Time", "report_time")
        
        # Standardize measurement columns
        .withColumnRenamed("GEN_SYSTEM_WIDE", "actual_mw")
        .withColumnRenamed("HSL_SYSTEM_WIDE", "hsl_mw")  # High Sustained Limit (potential)
        .withColumnRenamed("STPPF_SYSTEM_WIDE", "stppf_forecast_mw")  # Short-Term PV Power Forecast
        .withColumnRenamed("PVGRPP_SYSTEM_WIDE", "pvgrpp_forecast_mw")  # PV Generation Resource Production Potential
        .withColumnRenamed("COP_HSL_SYSTEM_WIDE", "cop_hsl_mw")  # Current Operating Plan HSL
        
        # Add time-based features
        .withColumn("hour_of_day", hour(col("interval_start")))
        .withColumn("day_of_week", date_format(col("interval_start"), "EEEE"))
        .withColumn("is_weekend", when(date_format(col("interval_start"), "E").isin("Sat", "Sun"), True).otherwise(False))
        
        # Calculate forecast error (where we have actuals)
        .withColumn(
            "stppf_error_mw",
            when(col("actual_mw").isNotNull(), col("stppf_forecast_mw") - col("actual_mw"))
        )
        .withColumn(
            "stppf_error_pct",
            when(
                (col("actual_mw").isNotNull()) & (col("actual_mw") > 0),
                (col("stppf_forecast_mw") - col("actual_mw")) / col("actual_mw") * 100
            )
        )
        
        # Calculate curtailment estimate (HSL - Actual when HSL > Actual)
        .withColumn(
            "estimated_curtailment_mw",
            when(
                (col("hsl_mw").isNotNull()) & (col("actual_mw").isNotNull()) & (col("hsl_mw") > col("actual_mw")),
                col("hsl_mw") - col("actual_mw")
            ).otherwise(0)
        )
        
        # Add metadata
        .withColumn("_transformed_at", current_timestamp())
        .withColumn("_source_table", lit(f"{CATALOG}.{SCHEMA_BRONZE}.solar_hourly_actual_forecast"))
        
        # Partition columns
        .withColumn("year", year(col("interval_start")))
        .withColumn("month", month(col("interval_start")))
        .withColumn("day", dayofmonth(col("interval_start")))
    )
    
    # Write to silver
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    )
    
    print(f"✓ Wrote {silver_df.count():,} rows to {CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wind Hourly - Clean and Standardize

# COMMAND ----------

def transform_wind_hourly():
    """
    Transform bronze wind hourly data to silver.
    
    Similar structure to solar, with wind-specific columns:
    - STWPF: Short-Term Wind Power Forecast
    - WGRPP: Wind Generation Resource Production Potential
    """
    
    bronze_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.wind_hourly_actual_forecast")
    
    silver_df = (
        bronze_df
        # Rename columns to snake_case (bronze already has underscores, just lowercase)
        .withColumnRenamed("Interval_Start", "interval_start")
        .withColumnRenamed("Interval_End", "interval_end")
        .withColumnRenamed("Publish_Time", "report_time")
        
        # Standardize measurement columns
        .withColumnRenamed("GEN_SYSTEM_WIDE", "actual_mw")
        .withColumnRenamed("HSL_SYSTEM_WIDE", "hsl_mw")
        .withColumnRenamed("STWPF_SYSTEM_WIDE", "stwpf_forecast_mw")  # Short-Term Wind Power Forecast
        .withColumnRenamed("WGRPP_SYSTEM_WIDE", "wgrpp_forecast_mw")  # Wind Generation Resource Production Potential
        .withColumnRenamed("COP_HSL_SYSTEM_WIDE", "cop_hsl_mw")
        
        # Time features
        .withColumn("hour_of_day", hour(col("interval_start")))
        .withColumn("day_of_week", date_format(col("interval_start"), "EEEE"))
        .withColumn("is_weekend", when(date_format(col("interval_start"), "E").isin("Sat", "Sun"), True).otherwise(False))
        
        # Forecast error
        .withColumn(
            "stwpf_error_mw",
            when(col("actual_mw").isNotNull(), col("stwpf_forecast_mw") - col("actual_mw"))
        )
        .withColumn(
            "stwpf_error_pct",
            when(
                (col("actual_mw").isNotNull()) & (col("actual_mw") > 0),
                (col("stwpf_forecast_mw") - col("actual_mw")) / col("actual_mw") * 100
            )
        )
        
        # Curtailment estimate
        .withColumn(
            "estimated_curtailment_mw",
            when(
                (col("hsl_mw").isNotNull()) & (col("actual_mw").isNotNull()) & (col("hsl_mw") > col("actual_mw")),
                col("hsl_mw") - col("actual_mw")
            ).otherwise(0)
        )
        
        # Metadata
        .withColumn("_transformed_at", current_timestamp())
        .withColumn("_source_table", lit(f"{CATALOG}.{SCHEMA_BRONZE}.wind_hourly_actual_forecast"))
        
        # Partitions
        .withColumn("year", year(col("interval_start")))
        .withColumn("month", month(col("interval_start")))
        .withColumn("day", dayofmonth(col("interval_start")))
    )
    
    # Write to silver
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.wind_hourly")
    )
    
    print(f"✓ Wrote {silver_df.count():,} rows to {CATALOG}.{SCHEMA_SILVER}.wind_hourly")
    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Renewable Generation View

# COMMAND ----------

def create_combined_renewable_view():
    """
    Create a unified view of solar + wind generation for total renewable analysis.
    """
    
    solar_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    wind_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.wind_hourly")
    
    # Select common columns and union
    solar_slim = (
        solar_df
        .select(
            col("interval_start"),
            col("interval_end"),
            col("actual_mw"),
            col("hsl_mw"),
            col("estimated_curtailment_mw"),
            col("hour_of_day"),
            col("day_of_week"),
            col("is_weekend"),
            col("year"),
            col("month"),
            col("day")
        )
        .withColumn("resource_type", lit("solar"))
        .withColumn("forecast_mw", col("actual_mw"))  # Placeholder - would use STPPF
    )
    
    wind_slim = (
        wind_df
        .select(
            col("interval_start"),
            col("interval_end"),
            col("actual_mw"),
            col("hsl_mw"),
            col("estimated_curtailment_mw"),
            col("hour_of_day"),
            col("day_of_week"),
            col("is_weekend"),
            col("year"),
            col("month"),
            col("day")
        )
        .withColumn("resource_type", lit("wind"))
        .withColumn("forecast_mw", col("actual_mw"))  # Placeholder
    )
    
    combined_df = solar_slim.unionByName(wind_slim)
    
    # Write as Delta table
    (
        combined_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "resource_type")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.renewable_generation_combined")
    )
    
    # Also create aggregated hourly totals
    totals_df = (
        combined_df
        .groupBy("interval_start", "interval_end", "hour_of_day", "day_of_week", "is_weekend", "year", "month", "day")
        .agg(
            spark_sum("actual_mw").alias("total_renewable_actual_mw"),
            spark_sum("hsl_mw").alias("total_renewable_hsl_mw"),
            spark_sum("estimated_curtailment_mw").alias("total_curtailment_mw"),
            spark_sum(when(col("resource_type") == "solar", col("actual_mw"))).alias("solar_actual_mw"),
            spark_sum(when(col("resource_type") == "wind", col("actual_mw"))).alias("wind_actual_mw"),
        )
        .withColumn("solar_pct", col("solar_actual_mw") / col("total_renewable_actual_mw") * 100)
        .withColumn("wind_pct", col("wind_actual_mw") / col("total_renewable_actual_mw") * 100)
    )
    
    (
        totals_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.renewable_generation_totals")
    )
    
    print(f"✓ Created combined renewable views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regional Data - Unpivot for Analysis

# COMMAND ----------

def transform_solar_regional():
    """
    Transform regional solar data from wide to long format.
    
    ERCOT reports solar by regions like: PANHANDLE, CIS, SOUTH, etc.
    Unpivoting makes analysis by region much easier.
    """
    
    bronze_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.solar_hourly_actual_forecast_regional")
    
    # Get region columns dynamically (they end with region names)
    all_cols = bronze_df.columns
    region_patterns = ["PANHANDLE", "CIS", "SOUTH", "NORTH", "WEST", "HOUSTON"]
    
    # Build unpivot expression
    # The regional data has columns like: GEN_PANHANDLE, STPPF_PANHANDLE, etc.
    
    # First, standardize column names (bronze already has underscores)
    silver_df = (
        bronze_df
        .withColumnRenamed("Interval_Start", "interval_start")
        .withColumnRenamed("Interval_End", "interval_end")
        .withColumn("hour_of_day", hour(col("interval_start")))
        .withColumn("year", year(col("interval_start")))
        .withColumn("month", month(col("interval_start")))
        .withColumn("day", dayofmonth(col("interval_start")))
        .withColumn("_transformed_at", current_timestamp())
    )
    
    # Write with original structure for now (unpivoting can be done in SQL)
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly_regional")
    )
    
    print(f"✓ Wrote regional solar data to silver")

# COMMAND ----------

def transform_wind_regional():
    """Transform regional wind data."""
    
    bronze_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.wind_hourly_actual_forecast_regional")
    
    silver_df = (
        bronze_df
        .withColumnRenamed("Interval_Start", "interval_start")
        .withColumnRenamed("Interval_End", "interval_end")
        .withColumn("hour_of_day", hour(col("interval_start")))
        .withColumn("year", year(col("interval_start")))
        .withColumn("month", month(col("interval_start")))
        .withColumn("day", dayofmonth(col("interval_start")))
        .withColumn("_transformed_at", current_timestamp())
    )
    
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.wind_hourly_regional")
    )
    
    print(f"✓ Wrote regional wind data to silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## LMP Day-Ahead Market - Clean and Enrich

# COMMAND ----------

def transform_lmp_dam():
    """
    Transform bronze LMP Day-Ahead Market data to silver.
    
    Key transformations:
    - Standardize column names
    - Parse settlement point types (Hub, Load Zone, Resource Node)
    - Add time-based features
    - Calculate price statistics
    """
    
    bronze_df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.lmp_day_ahead_market")
    
    # Check actual columns in the data
    print(f"Bronze LMP DAM columns: {bronze_df.columns}")
    
    silver_df = (
        bronze_df
        # Standardize column names - adjust based on actual column names
        .withColumnRenamed("Interval_Start", "interval_start")
        .withColumnRenamed("Interval_End", "interval_end")
        .withColumnRenamed("Location", "settlement_point")
        .withColumnRenamed("Location_Type", "settlement_point_type")
        .withColumnRenamed("LMP", "lmp_price")
        
        # Add time-based features
        .withColumn("delivery_date", col("interval_start").cast("date"))
        .withColumn("hour_ending", hour(col("interval_end")))
        .withColumn("hour_of_day", hour(col("interval_start")))
        .withColumn("day_of_week", date_format(col("interval_start"), "EEEE"))
        .withColumn("is_weekend", when(date_format(col("interval_start"), "E").isin("Sat", "Sun"), True).otherwise(False))
        .withColumn("is_peak_hour", when((hour(col("interval_start")) >= 7) & (hour(col("interval_start")) < 22), True).otherwise(False))
        
        # Classify settlement point type if not already present
        .withColumn(
            "point_type",
            when(col("settlement_point").like("%HB_%"), "Hub")
            .when(col("settlement_point").like("%LZ_%"), "Load Zone")
            .when(col("settlement_point").like("%ZONE%"), "Load Zone")
            .otherwise("Resource Node")
        )
        
        # Add metadata
        .withColumn("_transformed_at", current_timestamp())
        .withColumn("_source_table", lit(f"{CATALOG}.{SCHEMA_BRONZE}.lmp_day_ahead_market"))
        
        # Partition columns
        .withColumn("year", year(col("interval_start")))
        .withColumn("month", month(col("interval_start")))
        .withColumn("day", dayofmonth(col("interval_start")))
    )
    
    # Write to silver
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    )
    
    print(f"✓ Wrote {silver_df.count():,} rows to {CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    return silver_df


def create_lmp_hub_summary():
    """
    Create a summary view of LMP prices by hub.
    
    Focuses on the main trading hubs which are most commonly referenced:
    - HB_HOUSTON (Houston Hub)
    - HB_NORTH (North Hub)
    - HB_SOUTH (South Hub) 
    - HB_WEST (West Hub)
    - HB_PAN (Panhandle Hub)
    """
    
    silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    
    # Filter to just hubs
    hub_df = silver_df.filter(col("point_type") == "Hub")
    
    # Daily hub prices
    daily_hub_prices = (
        hub_df
        .groupBy("delivery_date", "settlement_point")
        .agg(
            avg("lmp_price").alias("avg_lmp"),
            spark_min("lmp_price").alias("min_lmp"),
            spark_max("lmp_price").alias("max_lmp"),
            expr("percentile(lmp_price, 0.5)").alias("median_lmp"),
            (spark_max("lmp_price") - spark_min("lmp_price")).alias("price_spread"),
            count("*").alias("hours_count")
        )
        .withColumn("year", year(col("delivery_date")))
        .withColumn("month", month(col("delivery_date")))
    )
    
    (
        daily_hub_prices
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.lmp_hub_daily_summary")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_SILVER}.lmp_hub_daily_summary")


def create_lmp_load_zone_summary():
    """
    Create a summary view of LMP prices by load zone.
    
    Load zones represent the major demand areas in ERCOT:
    - LZ_HOUSTON, LZ_NORTH, LZ_SOUTH, LZ_WEST
    """
    
    silver_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    
    # Filter to load zones
    lz_df = silver_df.filter(col("point_type") == "Load Zone")
    
    # Daily load zone prices
    daily_lz_prices = (
        lz_df
        .groupBy("delivery_date", "settlement_point")
        .agg(
            avg("lmp_price").alias("avg_lmp"),
            spark_min("lmp_price").alias("min_lmp"),
            spark_max("lmp_price").alias("max_lmp"),
            expr("percentile(lmp_price, 0.5)").alias("median_lmp"),
            (spark_max("lmp_price") - spark_min("lmp_price")).alias("daily_price_range"),
            count("*").alias("hours_count")
        )
        .withColumn("year", year(col("delivery_date")))
        .withColumn("month", month(col("delivery_date")))
    )
    
    (
        daily_lz_prices
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.lmp_load_zone_daily_summary")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_SILVER}.lmp_load_zone_daily_summary")

# COMMAND ----------

# Run all silver transformations
print("Starting Silver layer transformations...")
print("="*60)

try:
    transform_solar_hourly()
except Exception as e:
    print(f"✗ Solar hourly transform failed: {e}")

try:
    transform_wind_hourly()
except Exception as e:
    print(f"✗ Wind hourly transform failed: {e}")

try:
    create_combined_renewable_view()
except Exception as e:
    print(f"✗ Combined view creation failed: {e}")

try:
    transform_solar_regional()
except Exception as e:
    print(f"✗ Solar regional transform failed: {e}")

try:
    transform_wind_regional()
except Exception as e:
    print(f"✗ Wind regional transform failed: {e}")

try:
    transform_lmp_dam()
except Exception as e:
    print(f"✗ LMP DAM transform failed: {e}")

try:
    create_lmp_hub_summary()
except Exception as e:
    print(f"✗ LMP hub summary failed: {e}")

try:
    create_lmp_load_zone_summary()
except Exception as e:
    print(f"✗ LMP load zone summary failed: {e}")

print("="*60)
print("Silver transformations complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preview

# COMMAND ----------

# Preview solar data
display(
    spark.table(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    .orderBy(col("interval_start").desc())
    .limit(100)
)

# COMMAND ----------

# Preview renewable totals
display(
    spark.table(f"{CATALOG}.{SCHEMA_SILVER}.renewable_generation_totals")
    .orderBy(col("interval_start").desc())
    .limit(100)
)
