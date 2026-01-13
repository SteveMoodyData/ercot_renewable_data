# Databricks notebook source
# MAGIC %md
# MAGIC # ERCOT Renewable Data - Gold Layer
# MAGIC 
# MAGIC Business-ready aggregations and ML features for renewable energy forecasting.
# MAGIC 
# MAGIC ## Gold Tables
# MAGIC 1. **Forecast Accuracy Metrics** - Daily/hourly MAE, MAPE, bias by resource type
# MAGIC 2. **Generation Patterns** - Hourly/daily/monthly generation profiles
# MAGIC 3. **Curtailment Analysis** - When and how much renewable energy is curtailed
# MAGIC 4. **ML Feature Store** - Prepared features for forecast models

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, date_format, to_date,
    year, month, dayofmonth, hour, minute, dayofweek,
    expr, current_timestamp, datediff,
    avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    stddev, variance, percentile_approx, abs as spark_abs,
    lag, lead, row_number, dense_rank, first, last,
    collect_list, array, explode
)
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
CATALOG = "ercot_energy"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Forecast Accuracy Metrics

# COMMAND ----------

def create_solar_forecast_accuracy():
    """
    Calculate forecast accuracy metrics for solar generation.
    
    Metrics:
    - MAE (Mean Absolute Error)
    - MAPE (Mean Absolute Percentage Error)
    - RMSE (Root Mean Square Error)
    - Bias (systematic over/under forecasting)
    - Skill Score (vs persistence model)
    """
    
    solar_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    
    # Filter to rows where we have both actual and forecast
    accuracy_base = (
        solar_df
        .filter(col("actual_mw").isNotNull())
        .filter(col("stppf_forecast_mw").isNotNull())
        .filter(col("actual_mw") > 10)  # Filter out nighttime/low values
    )
    
    # Daily accuracy metrics
    daily_accuracy = (
        accuracy_base
        .groupBy(to_date(col("interval_start")).alias("date"))
        .agg(
            # MAE
            avg(spark_abs(col("stppf_error_mw"))).alias("mae_mw"),
            
            # MAPE (only where actual > 0)
            avg(
                when(col("actual_mw") > 0, 
                     spark_abs(col("stppf_error_mw")) / col("actual_mw") * 100)
            ).alias("mape_pct"),
            
            # RMSE
            expr("sqrt(avg(power(stppf_error_mw, 2)))").alias("rmse_mw"),
            
            # Bias (positive = over-forecast, negative = under-forecast)
            avg(col("stppf_error_mw")).alias("bias_mw"),
            
            # Volume metrics
            spark_sum(col("actual_mw")).alias("total_actual_mw"),
            spark_sum(col("stppf_forecast_mw")).alias("total_forecast_mw"),
            count("*").alias("observation_count"),
            
            # Peak metrics
            spark_max(col("actual_mw")).alias("peak_actual_mw"),
            spark_max(spark_abs(col("stppf_error_mw"))).alias("max_error_mw"),
        )
        .withColumn("resource_type", lit("solar"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )
    
    # Hourly pattern accuracy (by hour of day)
    hourly_pattern_accuracy = (
        accuracy_base
        .groupBy("hour_of_day")
        .agg(
            avg(spark_abs(col("stppf_error_mw"))).alias("avg_mae_mw"),
            avg(
                when(col("actual_mw") > 0, 
                     spark_abs(col("stppf_error_mw")) / col("actual_mw") * 100)
            ).alias("avg_mape_pct"),
            avg(col("stppf_error_mw")).alias("avg_bias_mw"),
            stddev(col("stppf_error_mw")).alias("error_stddev_mw"),
            count("*").alias("observation_count"),
        )
        .withColumn("resource_type", lit("solar"))
    )
    
    # Write daily accuracy
    (
        daily_accuracy
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.solar_forecast_accuracy_daily")
    )
    
    # Write hourly pattern accuracy
    (
        hourly_pattern_accuracy
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.solar_forecast_accuracy_by_hour")
    )
    
    print(f"✓ Created solar forecast accuracy tables")
    return daily_accuracy

# COMMAND ----------

def create_wind_forecast_accuracy():
    """Calculate forecast accuracy metrics for wind generation."""
    
    wind_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.wind_hourly")
    
    # Filter to rows where we have both actual and forecast
    accuracy_base = (
        wind_df
        .filter(col("actual_mw").isNotNull())
        .filter(col("stwpf_forecast_mw").isNotNull())
        .filter(col("actual_mw") > 100)  # Wind has higher base output
    )
    
    # Daily accuracy metrics
    daily_accuracy = (
        accuracy_base
        .groupBy(to_date(col("interval_start")).alias("date"))
        .agg(
            avg(spark_abs(col("stwpf_error_mw"))).alias("mae_mw"),
            avg(
                when(col("actual_mw") > 0, 
                     spark_abs(col("stwpf_error_mw")) / col("actual_mw") * 100)
            ).alias("mape_pct"),
            expr("sqrt(avg(power(stwpf_error_mw, 2)))").alias("rmse_mw"),
            avg(col("stwpf_error_mw")).alias("bias_mw"),
            spark_sum(col("actual_mw")).alias("total_actual_mw"),
            spark_sum(col("stwpf_forecast_mw")).alias("total_forecast_mw"),
            count("*").alias("observation_count"),
            spark_max(col("actual_mw")).alias("peak_actual_mw"),
            spark_max(spark_abs(col("stwpf_error_mw"))).alias("max_error_mw"),
        )
        .withColumn("resource_type", lit("wind"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )
    
    # Hourly pattern accuracy
    hourly_pattern_accuracy = (
        accuracy_base
        .groupBy("hour_of_day")
        .agg(
            avg(spark_abs(col("stwpf_error_mw"))).alias("avg_mae_mw"),
            avg(
                when(col("actual_mw") > 0, 
                     spark_abs(col("stwpf_error_mw")) / col("actual_mw") * 100)
            ).alias("avg_mape_pct"),
            avg(col("stwpf_error_mw")).alias("avg_bias_mw"),
            stddev(col("stwpf_error_mw")).alias("error_stddev_mw"),
            count("*").alias("observation_count"),
        )
        .withColumn("resource_type", lit("wind"))
    )
    
    # Write tables
    (
        daily_accuracy
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.wind_forecast_accuracy_daily")
    )
    
    (
        hourly_pattern_accuracy
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.wind_forecast_accuracy_by_hour")
    )
    
    print(f"✓ Created wind forecast accuracy tables")
    return daily_accuracy

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generation Patterns

# COMMAND ----------

def create_generation_patterns():
    """
    Create aggregated generation pattern tables for analysis.
    
    Useful for:
    - Understanding typical daily/seasonal patterns
    - Capacity planning
    - Identifying anomalies
    """
    
    totals_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.renewable_generation_totals")
    
    # Hourly average patterns (typical day profile)
    hourly_patterns = (
        totals_df
        .groupBy("hour_of_day")
        .agg(
            avg(col("total_renewable_actual_mw")).alias("avg_total_mw"),
            avg(col("solar_actual_mw")).alias("avg_solar_mw"),
            avg(col("wind_actual_mw")).alias("avg_wind_mw"),
            
            percentile_approx(col("total_renewable_actual_mw"), 0.10).alias("p10_total_mw"),
            percentile_approx(col("total_renewable_actual_mw"), 0.50).alias("p50_total_mw"),
            percentile_approx(col("total_renewable_actual_mw"), 0.90).alias("p90_total_mw"),
            
            spark_max(col("total_renewable_actual_mw")).alias("max_total_mw"),
            spark_min(col("total_renewable_actual_mw")).alias("min_total_mw"),
            
            stddev(col("total_renewable_actual_mw")).alias("stddev_total_mw"),
            count("*").alias("observation_count"),
        )
    )
    
    # Monthly totals
    monthly_totals = (
        totals_df
        .groupBy("year", "month")
        .agg(
            spark_sum(col("total_renewable_actual_mw")).alias("total_generation_mwh"),
            spark_sum(col("solar_actual_mw")).alias("solar_generation_mwh"),
            spark_sum(col("wind_actual_mw")).alias("wind_generation_mwh"),
            spark_sum(col("total_curtailment_mw")).alias("total_curtailment_mwh"),
            
            spark_max(col("total_renewable_actual_mw")).alias("peak_hour_mw"),
            avg(col("total_renewable_actual_mw")).alias("avg_hour_mw"),
            
            count("*").alias("hours_in_month"),
        )
        .withColumn(
            "capacity_factor_estimate",
            col("avg_hour_mw") / col("peak_hour_mw") * 100
        )
        .withColumn(
            "solar_share_pct",
            col("solar_generation_mwh") / col("total_generation_mwh") * 100
        )
        .withColumn(
            "wind_share_pct",
            col("wind_generation_mwh") / col("total_generation_mwh") * 100
        )
    )
    
    # Daily patterns by day of week
    dow_patterns = (
        totals_df
        .groupBy("day_of_week", "hour_of_day")
        .agg(
            avg(col("total_renewable_actual_mw")).alias("avg_total_mw"),
            avg(col("solar_actual_mw")).alias("avg_solar_mw"),
            avg(col("wind_actual_mw")).alias("avg_wind_mw"),
        )
    )
    
    # Write tables
    (
        hourly_patterns
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.generation_hourly_patterns")
    )
    
    (
        monthly_totals
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.generation_monthly_summary")
    )
    
    (
        dow_patterns
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.generation_dow_patterns")
    )
    
    print("✓ Created generation pattern tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Curtailment Analysis

# COMMAND ----------

def create_curtailment_analysis():
    """
    Analyze renewable curtailment patterns.
    
    Curtailment = potential generation (HSL) - actual generation
    Happens when grid can't absorb all renewable energy.
    """
    
    solar_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    wind_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.wind_hourly")
    
    # Solar curtailment by hour
    solar_curtailment = (
        solar_df
        .filter(col("estimated_curtailment_mw") > 0)
        .groupBy("hour_of_day")
        .agg(
            avg(col("estimated_curtailment_mw")).alias("avg_curtailment_mw"),
            spark_sum(col("estimated_curtailment_mw")).alias("total_curtailment_mwh"),
            count("*").alias("curtailment_hours"),
            spark_max(col("estimated_curtailment_mw")).alias("max_curtailment_mw"),
        )
        .withColumn("resource_type", lit("solar"))
    )
    
    # Wind curtailment by hour
    wind_curtailment = (
        wind_df
        .filter(col("estimated_curtailment_mw") > 0)
        .groupBy("hour_of_day")
        .agg(
            avg(col("estimated_curtailment_mw")).alias("avg_curtailment_mw"),
            spark_sum(col("estimated_curtailment_mw")).alias("total_curtailment_mwh"),
            count("*").alias("curtailment_hours"),
            spark_max(col("estimated_curtailment_mw")).alias("max_curtailment_mw"),
        )
        .withColumn("resource_type", lit("wind"))
    )
    
    # Combined curtailment patterns
    combined_curtailment = solar_curtailment.unionByName(wind_curtailment)
    
    (
        combined_curtailment
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.curtailment_hourly_patterns")
    )
    
    # Daily curtailment summary
    daily_curtailment = (
        solar_df
        .select(
            to_date(col("interval_start")).alias("date"),
            col("estimated_curtailment_mw").alias("solar_curtailment_mw"),
            col("actual_mw").alias("solar_actual_mw"),
            col("hsl_mw").alias("solar_hsl_mw"),
        )
        .join(
            wind_df
            .select(
                to_date(col("interval_start")).alias("date"),
                col("estimated_curtailment_mw").alias("wind_curtailment_mw"),
                col("actual_mw").alias("wind_actual_mw"),
                col("hsl_mw").alias("wind_hsl_mw"),
            ),
            "date"
        )
        .groupBy("date")
        .agg(
            spark_sum(col("solar_curtailment_mw")).alias("solar_curtailment_mwh"),
            spark_sum(col("wind_curtailment_mw")).alias("wind_curtailment_mwh"),
            spark_sum(col("solar_actual_mw")).alias("solar_generation_mwh"),
            spark_sum(col("wind_actual_mw")).alias("wind_generation_mwh"),
        )
        .withColumn(
            "total_curtailment_mwh",
            col("solar_curtailment_mwh") + col("wind_curtailment_mwh")
        )
        .withColumn(
            "total_generation_mwh",
            col("solar_generation_mwh") + col("wind_generation_mwh")
        )
        .withColumn(
            "curtailment_rate_pct",
            col("total_curtailment_mwh") / (col("total_generation_mwh") + col("total_curtailment_mwh")) * 100
        )
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )
    
    (
        daily_curtailment
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.curtailment_daily_summary")
    )
    
    print("✓ Created curtailment analysis tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ML Feature Store

# COMMAND ----------

def create_ml_features():
    """
    Create feature table for renewable generation forecasting ML models.
    
    Features include:
    - Lagged generation values
    - Rolling averages
    - Time-based features
    - Seasonality indicators
    """
    
    solar_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.solar_hourly")
    
    # Define window specs
    window_1h = Window.orderBy("interval_start").rowsBetween(-1, -1)
    window_24h = Window.orderBy("interval_start").rowsBetween(-24, -1)
    window_168h = Window.orderBy("interval_start").rowsBetween(-168, -1)  # 1 week
    
    features_df = (
        solar_df
        .filter(col("actual_mw").isNotNull())
        
        # Lagged features
        .withColumn("actual_lag_1h", lag(col("actual_mw"), 1).over(Window.orderBy("interval_start")))
        .withColumn("actual_lag_24h", lag(col("actual_mw"), 24).over(Window.orderBy("interval_start")))
        .withColumn("actual_lag_168h", lag(col("actual_mw"), 168).over(Window.orderBy("interval_start")))
        
        # Rolling averages
        .withColumn("actual_avg_24h", avg(col("actual_mw")).over(window_24h))
        .withColumn("actual_avg_168h", avg(col("actual_mw")).over(window_168h))
        
        # Rolling max/min
        .withColumn("actual_max_24h", spark_max(col("actual_mw")).over(window_24h))
        .withColumn("actual_min_24h", spark_min(col("actual_mw")).over(window_24h))
        
        # Volatility (rolling std)
        .withColumn("actual_std_24h", stddev(col("actual_mw")).over(window_24h))
        
        # Time features (cyclical encoding would be done in model training)
        .withColumn("hour_sin", expr("sin(2 * pi() * hour_of_day / 24)"))
        .withColumn("hour_cos", expr("cos(2 * pi() * hour_of_day / 24)"))
        .withColumn("day_of_year", dayofmonth(col("interval_start")))
        .withColumn("month_num", month(col("interval_start")))
        .withColumn("day_of_week_num", dayofweek(col("interval_start")))
        
        # Same hour yesterday/last week
        .withColumn("same_hour_yesterday", lag(col("actual_mw"), 24).over(Window.orderBy("interval_start")))
        .withColumn("same_hour_last_week", lag(col("actual_mw"), 168).over(Window.orderBy("interval_start")))
        
        # Target variable
        .withColumn("target_actual_mw", col("actual_mw"))
        
        # Filter out rows without enough history
        .filter(col("actual_lag_168h").isNotNull())
        
        # Select final feature columns
        .select(
            "interval_start",
            "hour_of_day",
            "is_weekend",
            "actual_lag_1h",
            "actual_lag_24h",
            "actual_lag_168h",
            "actual_avg_24h",
            "actual_avg_168h",
            "actual_max_24h",
            "actual_min_24h",
            "actual_std_24h",
            "hour_sin",
            "hour_cos",
            "day_of_year",
            "month_num",
            "day_of_week_num",
            "same_hour_yesterday",
            "same_hour_last_week",
            "stppf_forecast_mw",  # Include ERCOT's forecast as a feature
            "target_actual_mw",
            "year",
            "month",
            "day",
        )
    )
    
    (
        features_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.solar_forecast_features")
    )
    
    print(f"✓ Created ML feature table with {features_df.count():,} rows")
    return features_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. LMP Price Analytics

# COMMAND ----------

def create_lmp_price_statistics():
    """
    Create comprehensive LMP price statistics.
    
    Tables created:
    - lmp_daily_statistics: Daily price stats by hub/zone
    - lmp_hourly_patterns: Average prices by hour of day
    """
    
    lmp_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    
    # === Daily Statistics by Settlement Point ===
    daily_stats = (
        lmp_df
        .groupBy(
            col("delivery_date").alias("date"),
            "settlement_point",
            "point_type"
        )
        .agg(
            avg("lmp_price").alias("avg_price"),
            spark_min("lmp_price").alias("min_price"),
            spark_max("lmp_price").alias("max_price"),
            expr("percentile(lmp_price, 0.25)").alias("p25_price"),
            expr("percentile(lmp_price, 0.5)").alias("median_price"),
            expr("percentile(lmp_price, 0.75)").alias("p75_price"),
            stddev("lmp_price").alias("stddev_price"),
            (spark_max("lmp_price") - spark_min("lmp_price")).alias("daily_range"),
            count("*").alias("hours_count"),
            # Peak vs off-peak
            avg(when(col("is_peak_hour"), col("lmp_price"))).alias("avg_peak_price"),
            avg(when(~col("is_peak_hour"), col("lmp_price"))).alias("avg_offpeak_price")
        )
        .withColumn("peak_offpeak_spread", col("avg_peak_price") - col("avg_offpeak_price"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )
    
    (
        daily_stats
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_daily_statistics")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_daily_statistics")
    
    # === Hourly Price Patterns ===
    hourly_patterns = (
        lmp_df
        .filter(col("point_type") == "Hub")  # Focus on hubs for pattern analysis
        .groupBy("settlement_point", "hour_of_day")
        .agg(
            avg("lmp_price").alias("avg_price"),
            spark_min("lmp_price").alias("min_price"),
            spark_max("lmp_price").alias("max_price"),
            expr("percentile(lmp_price, 0.1)").alias("p10_price"),
            expr("percentile(lmp_price, 0.5)").alias("median_price"),
            expr("percentile(lmp_price, 0.9)").alias("p90_price"),
            stddev("lmp_price").alias("stddev_price"),
            count("*").alias("observation_count")
        )
    )
    
    (
        hourly_patterns
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_hourly_patterns")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_hourly_patterns")


def create_lmp_hub_comparison():
    """
    Create hub-to-hub price comparison and spread analysis.
    
    Useful for:
    - Identifying congestion patterns
    - Trading strategy development
    - Basis risk analysis
    """
    
    lmp_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    
    # Get hub prices pivoted for comparison
    hub_prices = (
        lmp_df
        .filter(col("point_type") == "Hub")
        .select("interval_start", "delivery_date", "hour_of_day", "settlement_point", "lmp_price")
    )
    
    # Pivot to get hubs as columns
    hub_pivot = (
        hub_prices
        .groupBy("delivery_date", "hour_of_day")
        .pivot("settlement_point")
        .agg(first("lmp_price"))
    )
    
    # Calculate common spreads (if hubs exist)
    hub_cols = [c for c in hub_pivot.columns if c.startswith("HB_")]
    
    if "HB_HOUSTON" in hub_cols and "HB_WEST" in hub_cols:
        hub_pivot = hub_pivot.withColumn(
            "houston_west_spread",
            col("HB_HOUSTON") - col("HB_WEST")
        )
    
    if "HB_NORTH" in hub_cols and "HB_SOUTH" in hub_cols:
        hub_pivot = hub_pivot.withColumn(
            "north_south_spread",
            col("HB_NORTH") - col("HB_SOUTH")
        )
    
    hub_pivot = (
        hub_pivot
        .withColumn("year", year(col("delivery_date")))
        .withColumn("month", month(col("delivery_date")))
    )
    
    (
        hub_pivot
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_hub_hourly_comparison")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_hub_hourly_comparison")
    
    # === Daily spread summary ===
    spread_cols = [c for c in hub_pivot.columns if "spread" in c]
    if spread_cols:
        agg_exprs = [
            avg(c).alias(f"avg_{c}") for c in spread_cols
        ] + [
            spark_min(spread_cols[0]).alias(f"min_{spread_cols[0]}"),
            spark_max(spread_cols[0]).alias(f"max_{spread_cols[0]}")
        ]
        
        daily_spreads = (
            hub_pivot
            .groupBy("delivery_date")
            .agg(*agg_exprs)
            .withColumn("year", year(col("delivery_date")))
            .withColumn("month", month(col("delivery_date")))
        )
        
        (
            daily_spreads
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_daily_spreads")
        )
        
        print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_daily_spreads")


def create_lmp_volatility_metrics():
    """
    Calculate price volatility metrics for risk analysis.
    
    Metrics:
    - Rolling volatility (7-day, 30-day)
    - Price spike frequency
    - Extreme price events
    """
    
    daily_stats = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.lmp_daily_statistics")
    
    # Focus on main hubs
    hub_daily = daily_stats.filter(col("point_type") == "Hub")
    
    # Add rolling volatility
    window_7d = Window.partitionBy("settlement_point").orderBy("date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("settlement_point").orderBy("date").rowsBetween(-29, 0)
    
    volatility_df = (
        hub_daily
        .withColumn("rolling_7d_avg", avg("avg_price").over(window_7d))
        .withColumn("rolling_7d_stddev", stddev("avg_price").over(window_7d))
        .withColumn("rolling_30d_avg", avg("avg_price").over(window_30d))
        .withColumn("rolling_30d_stddev", stddev("avg_price").over(window_30d))
        
        # Coefficient of variation (volatility normalized by mean)
        .withColumn("cv_7d", col("rolling_7d_stddev") / col("rolling_7d_avg") * 100)
        .withColumn("cv_30d", col("rolling_30d_stddev") / col("rolling_30d_avg") * 100)
        
        # Price spike indicator (price > 2x 30-day average)
        .withColumn("is_price_spike", col("max_price") > (col("rolling_30d_avg") * 2))
        
        # Extreme price indicator (any hour > $100 or < $0)
        .withColumn("has_extreme_high", col("max_price") > 100)
        .withColumn("has_negative_price", col("min_price") < 0)
    )
    
    (
        volatility_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_volatility_metrics")
    )
    
    print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_volatility_metrics")


def create_lmp_renewable_correlation():
    """
    Analyze correlation between renewable generation and LMP prices.
    
    Key insights:
    - How do high wind/solar periods affect prices?
    - Price impact of renewable curtailment
    - Negative price events and renewable generation
    """
    
    # Join LMP with renewable generation
    lmp_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.lmp_day_ahead_market")
    
    # Get hub prices (use Houston as reference)
    hub_lmp = (
        lmp_df
        .filter(col("settlement_point").like("%HOUSTON%"))
        .select(
            col("interval_start"),
            col("delivery_date"),
            col("hour_of_day"),
            col("lmp_price").alias("houston_lmp")
        )
    )
    
    try:
        renewable_df = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.renewable_generation_totals")
        
        # Join on interval
        combined = (
            hub_lmp
            .join(
                renewable_df.select(
                    "interval_start",
                    "total_actual_mw",
                    "solar_actual_mw",
                    "wind_actual_mw",
                    "total_curtailment_mw"
                ),
                on="interval_start",
                how="inner"
            )
        )
        
        # Aggregate by day
        daily_correlation = (
            combined
            .groupBy("delivery_date")
            .agg(
                avg("houston_lmp").alias("avg_lmp"),
                avg("total_actual_mw").alias("avg_renewable_mw"),
                avg("solar_actual_mw").alias("avg_solar_mw"),
                avg("wind_actual_mw").alias("avg_wind_mw"),
                avg("total_curtailment_mw").alias("avg_curtailment_mw"),
                
                # Count negative price hours
                spark_sum(when(col("houston_lmp") < 0, 1).otherwise(0)).alias("negative_price_hours"),
                
                count("*").alias("hours_count")
            )
            .withColumn("year", year(col("delivery_date")))
            .withColumn("month", month(col("delivery_date")))
        )
        
        (
            daily_correlation
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.lmp_renewable_correlation")
        )
        
        print(f"✓ Created {CATALOG}.{SCHEMA_GOLD}.lmp_renewable_correlation")
        
    except Exception as e:
        print(f"⚠ Could not create renewable correlation (renewable data may not exist): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Transformations

# COMMAND ----------

# Run all gold layer transformations
print("Starting Gold layer transformations...")
print("="*60)

try:
    create_solar_forecast_accuracy()
except Exception as e:
    print(f"✗ Solar accuracy failed: {e}")

try:
    create_wind_forecast_accuracy()
except Exception as e:
    print(f"✗ Wind accuracy failed: {e}")

try:
    create_generation_patterns()
except Exception as e:
    print(f"✗ Generation patterns failed: {e}")

try:
    create_curtailment_analysis()
except Exception as e:
    print(f"✗ Curtailment analysis failed: {e}")

try:
    create_ml_features()
except Exception as e:
    print(f"✗ ML features failed: {e}")

try:
    create_lmp_price_statistics()
except Exception as e:
    print(f"✗ LMP price statistics failed: {e}")

try:
    create_lmp_hub_comparison()
except Exception as e:
    print(f"✗ LMP hub comparison failed: {e}")

try:
    create_lmp_volatility_metrics()
except Exception as e:
    print(f"✗ LMP volatility metrics failed: {e}")

try:
    create_lmp_renewable_correlation()
except Exception as e:
    print(f"✗ LMP renewable correlation failed: {e}")

print("="*60)
print("Gold transformations complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Forecast accuracy trend
# MAGIC SELECT 
# MAGIC   date,
# MAGIC   mae_mw,
# MAGIC   mape_pct,
# MAGIC   bias_mw,
# MAGIC   observation_count
# MAGIC FROM ercot_energy.gold.solar_forecast_accuracy_daily
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hourly generation pattern
# MAGIC SELECT 
# MAGIC   hour_of_day,
# MAGIC   ROUND(avg_solar_mw, 0) as avg_solar_mw,
# MAGIC   ROUND(avg_wind_mw, 0) as avg_wind_mw,
# MAGIC   ROUND(avg_total_mw, 0) as avg_total_mw,
# MAGIC   ROUND(p10_total_mw, 0) as p10_mw,
# MAGIC   ROUND(p90_total_mw, 0) as p90_mw
# MAGIC FROM ercot_energy.gold.generation_hourly_patterns
# MAGIC ORDER BY hour_of_day

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly summary
# MAGIC SELECT 
# MAGIC   year,
# MAGIC   month,
# MAGIC   ROUND(total_generation_mwh / 1000, 1) as total_gwh,
# MAGIC   ROUND(solar_share_pct, 1) as solar_pct,
# MAGIC   ROUND(wind_share_pct, 1) as wind_pct,
# MAGIC   ROUND(total_curtailment_mwh / 1000, 1) as curtailment_gwh
# MAGIC FROM ercot_energy.gold.generation_monthly_summary
# MAGIC ORDER BY year DESC, month DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LMP Daily Statistics by Hub
# MAGIC SELECT 
# MAGIC   date,
# MAGIC   settlement_point,
# MAGIC   ROUND(avg_price, 2) as avg_price,
# MAGIC   ROUND(min_price, 2) as min_price,
# MAGIC   ROUND(max_price, 2) as max_price,
# MAGIC   ROUND(peak_offpeak_spread, 2) as peak_offpeak_spread
# MAGIC FROM ercot_energy.gold.lmp_daily_statistics
# MAGIC WHERE point_type = 'Hub'
# MAGIC ORDER BY date DESC, settlement_point
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LMP Hourly Price Patterns by Hub
# MAGIC SELECT 
# MAGIC   settlement_point,
# MAGIC   hour_of_day,
# MAGIC   ROUND(avg_price, 2) as avg_price,
# MAGIC   ROUND(p10_price, 2) as p10_price,
# MAGIC   ROUND(p90_price, 2) as p90_price,
# MAGIC   observation_count
# MAGIC FROM ercot_energy.gold.lmp_hourly_patterns
# MAGIC ORDER BY settlement_point, hour_of_day

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Price Volatility Analysis
# MAGIC SELECT 
# MAGIC   date,
# MAGIC   settlement_point,
# MAGIC   ROUND(avg_price, 2) as avg_price,
# MAGIC   ROUND(rolling_7d_stddev, 2) as volatility_7d,
# MAGIC   ROUND(cv_7d, 1) as cv_7d_pct,
# MAGIC   is_price_spike,
# MAGIC   has_negative_price
# MAGIC FROM ercot_energy.gold.lmp_volatility_metrics
# MAGIC WHERE date >= current_date() - 30
# MAGIC ORDER BY date DESC, settlement_point
