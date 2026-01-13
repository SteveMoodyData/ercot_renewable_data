# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Tests
# MAGIC
# MAGIC This notebook tests critical functions in the bronze ingestion layer:
# MAGIC 1. `table_exists()` - Verify table detection works correctly
# MAGIC 2. `determine_load_dates()` - Test incremental load date logic
# MAGIC 3. `write_to_bronze()` - Verify partitioning and data writes
# MAGIC 4. `run_bronze_quality_checks()` - Test data quality validation
# MAGIC
# MAGIC ## Setup
# MAGIC - Uses test catalog: `ercot_energy_test`
# MAGIC - All test tables are cleaned up after tests
# MAGIC - Can be run manually in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# Test environment configuration
TEST_CATALOG = "ercot_energy_test"
TEST_SCHEMA_BRONZE = "bronze"

# Override the production catalog for testing
CATALOG = TEST_CATALOG
SCHEMA_BRONZE = TEST_SCHEMA_BRONZE

print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Test Environment

# COMMAND ----------

# Create test catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA_BRONZE}")

print(f"✓ Created test catalog: {TEST_CATALOG}")
print(f"✓ Created test schema: {TEST_CATALOG}.{TEST_SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Functions to Test
# MAGIC
# MAGIC Copy the functions from bronze ingestion notebook that we want to test.
# MAGIC In a real setup, you'd use `%run ./01_bronze_ingestion` to import them.

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    """Check if a table exists in the catalog."""
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def get_last_loaded_date(table_name: str):
    """Get the most recent date loaded for incremental processing."""
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"

    # First check if table exists
    if not table_exists(table_name):
        print(f"Table {full_table_name} does not exist yet - will perform initial load")
        return None

    try:
        result = spark.sql(f"""
            SELECT MAX(DATE(Interval_Start)) as max_date
            FROM {full_table_name}
        """).collect()

        if result and result[0]["max_date"]:
            return result[0]["max_date"]
    except Exception as e:
        print(f"Could not get last loaded date from {full_table_name}: {e}")

    return None


def determine_load_dates(
    table_name: str,
    lookback_days: int = 7,
    default_start: str = "2024-01-01"
) -> tuple:
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

    # Add partition columns if time-based (columns now have underscores)
    if "Interval_Start" in spark_df.columns:
        spark_df = (
            spark_df
            .withColumn("year", year(col("Interval_Start")))
            .withColumn("month", month(col("Interval_Start")))
            .withColumn("day", dayofmonth(col("Interval_Start")))
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


def run_bronze_quality_checks(table_name: str) -> dict:
    """Run basic quality checks on bronze table."""
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"

    # Check if table exists first
    if not table_exists(table_name):
        return {"status": "table_not_found"}

    checks = {}

    # Row count
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]["cnt"]
    checks["row_count"] = count

    if count == 0:
        return checks

    # Determine the timestamp column (different tables use different names)
    df_columns = spark.table(full_table_name).columns
    if "Interval_Start" in df_columns:
        time_col = "Interval_Start"
    elif "Time" in df_columns:
        time_col = "Time"
    else:
        # No recognized time column, skip time-based checks
        checks["min_date"] = "N/A"
        checks["max_date"] = "N/A"
        checks["null_time_column"] = "N/A"
        checks["duplicate_intervals"] = "N/A"
        return checks

    # Date range
    date_range = spark.sql(f"""
        SELECT
            MIN(DATE({time_col})) as min_date,
            MAX(DATE({time_col})) as max_date
        FROM {full_table_name}
    """).collect()[0]
    checks["min_date"] = str(date_range["min_date"])
    checks["max_date"] = str(date_range["max_date"])

    # Null check on key columns
    null_check = spark.sql(f"""
        SELECT
            SUM(CASE WHEN {time_col} IS NULL THEN 1 ELSE 0 END) as null_time_column,
            SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END) as null_ingested_at
        FROM {full_table_name}
    """).collect()[0]
    checks["null_time_column"] = null_check["null_time_column"]
    checks["null_ingested_at"] = null_check["null_ingested_at"]

    # Duplicate check
    dup_check = spark.sql(f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT {time_col}, COUNT(*) as cnt
            FROM {full_table_name}
            GROUP BY {time_col}
            HAVING COUNT(*) > 1
        )
    """).collect()[0]
    checks["duplicate_intervals"] = dup_check["dup_count"]

    return checks

print("✓ Functions loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: table_exists()
# MAGIC
# MAGIC Tests that we can correctly detect existing and non-existing tables.

# COMMAND ----------

def test_table_exists():
    """Test table_exists() with real Databricks tables"""
    print("Running Test 1: table_exists()")
    print("-" * 60)

    test_table_name = "test_exists_table"
    full_table_name = f"{TEST_CATALOG}.{TEST_SCHEMA_BRONZE}.{test_table_name}"

    # Clean up if exists from previous run
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    except:
        pass

    # Test 1: Should return False for non-existent table
    result = table_exists(test_table_name)
    assert result == False, "Should return False for non-existent table"
    print(f"  ✓ Non-existent table correctly returns False")

    # Create test table
    spark.sql(f"""
        CREATE TABLE {full_table_name} (
            id INT,
            value STRING
        ) USING DELTA
    """)
    print(f"  ✓ Created test table: {full_table_name}")

    # Test 2: Should return True for existing table
    result = table_exists(test_table_name)
    assert result == True, "Should return True for existing table"
    print(f"  ✓ Existing table correctly returns True")

    # Cleanup
    spark.sql(f"DROP TABLE {full_table_name}")
    print(f"  ✓ Cleaned up test table")

    print("\n✅ Test 1 PASSED: table_exists() works correctly\n")
    return True

# Run the test
try:
    test_table_exists()
except AssertionError as e:
    print(f"\n❌ Test 1 FAILED: {e}\n")
except Exception as e:
    print(f"\n❌ Test 1 ERROR: {e}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: determine_load_dates()
# MAGIC
# MAGIC Tests incremental load date calculation logic.

# COMMAND ----------

def test_determine_load_dates():
    """Test determine_load_dates() logic for initial and incremental loads"""
    print("Running Test 2: determine_load_dates()")
    print("-" * 60)

    test_table_name = "test_load_dates"
    full_table_name = f"{TEST_CATALOG}.{TEST_SCHEMA_BRONZE}.{test_table_name}"

    # Clean up if exists from previous run
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    except:
        pass

    # Test 1: Initial load (table doesn't exist)
    print("  Testing initial load scenario...")
    start, end = determine_load_dates(test_table_name, default_start="2024-01-01")
    assert start == "2024-01-01", f"Initial load should use default_start, got {start}"
    print(f"  ✓ Initial load date range: {start} to {end}")

    # Create table with some historical data
    print("  Creating table with historical data...")
    test_df = spark.createDataFrame([
        (datetime(2024, 12, 1, 0, 0), 100.0),
        (datetime(2024, 12, 2, 0, 0), 200.0),
        (datetime(2024, 12, 3, 0, 0), 300.0),
    ], ["Interval_Start", "value"])

    test_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
    print(f"  ✓ Created table with data from 2024-12-01 to 2024-12-03")

    # Test 2: Incremental load (table exists with data)
    print("  Testing incremental load scenario...")
    start, end = determine_load_dates(test_table_name, lookback_days=7)

    # Should go back 7 days from last loaded date (2024-12-03)
    expected_start = (datetime(2024, 12, 3) - timedelta(days=7)).strftime("%Y-%m-%d")
    assert start == expected_start, f"Expected {expected_start}, got {start}"
    print(f"  ✓ Incremental load date range: {start} to {end}")
    print(f"  ✓ Correctly went back 7 days from last loaded date (2024-12-03)")

    # Test 3: Custom lookback period
    print("  Testing custom lookback period...")
    start, end = determine_load_dates(test_table_name, lookback_days=3)
    expected_start = (datetime(2024, 12, 3) - timedelta(days=3)).strftime("%Y-%m-%d")
    assert start == expected_start, f"Expected {expected_start}, got {start}"
    print(f"  ✓ Custom lookback (3 days): {start} to {end}")

    # Cleanup
    spark.sql(f"DROP TABLE {full_table_name}")
    print(f"  ✓ Cleaned up test table")

    print("\n✅ Test 2 PASSED: determine_load_dates() works correctly\n")
    return True

# Run the test
try:
    test_determine_load_dates()
except AssertionError as e:
    print(f"\n❌ Test 2 FAILED: {e}\n")
except Exception as e:
    print(f"\n❌ Test 2 ERROR: {e}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: write_to_bronze()
# MAGIC
# MAGIC Tests that data writes correctly with proper partitioning.

# COMMAND ----------

def test_write_to_bronze():
    """Test write_to_bronze() creates partitions correctly"""
    print("Running Test 3: write_to_bronze()")
    print("-" * 60)

    test_table_name = "test_write_bronze"
    full_table_name = f"{TEST_CATALOG}.{TEST_SCHEMA_BRONZE}.{test_table_name}"

    # Clean up if exists from previous run
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    except:
        pass

    # Test 1: Write data with Interval_Start column
    print("  Creating sample data with Interval_Start column...")
    sample_data = pd.DataFrame({
        "Interval_Start": [
            datetime(2024, 1, 15, 10, 0),
            datetime(2024, 2, 20, 14, 0),
            datetime(2024, 3, 25, 18, 0),
        ],
        "value": [100.0, 200.0, 300.0],
        "_ingested_at": [datetime.utcnow(), datetime.utcnow(), datetime.utcnow()],
        "_source": ["test", "test", "test"],
        "_endpoint": ["/test", "/test", "/test"]
    })

    # Write to test table
    print("  Writing data to bronze table...")
    write_to_bronze(
        df=sample_data,
        table_name=test_table_name,
        partition_cols=["year", "month", "day"],
        mode="overwrite"
    )

    # Read back and verify
    print("  Verifying written data...")
    result_df = spark.table(full_table_name)

    # Check partition columns were created
    assert "year" in result_df.columns, "year partition column missing"
    assert "month" in result_df.columns, "month partition column missing"
    assert "day" in result_df.columns, "day partition column missing"
    print("  ✓ Partition columns (year, month, day) created")

    # Check partition values
    result = result_df.orderBy("Interval_Start").collect()

    # First row: 2024-01-15
    assert result[0]["year"] == 2024, f"Expected year 2024, got {result[0]['year']}"
    assert result[0]["month"] == 1, f"Expected month 1, got {result[0]['month']}"
    assert result[0]["day"] == 15, f"Expected day 15, got {result[0]['day']}"
    print("  ✓ Row 1 partitions correct: 2024-01-15")

    # Second row: 2024-02-20
    assert result[1]["year"] == 2024, f"Expected year 2024, got {result[1]['year']}"
    assert result[1]["month"] == 2, f"Expected month 2, got {result[1]['month']}"
    assert result[1]["day"] == 20, f"Expected day 20, got {result[1]['day']}"
    print("  ✓ Row 2 partitions correct: 2024-02-20")

    # Third row: 2024-03-25
    assert result[2]["year"] == 2024, f"Expected year 2024, got {result[2]['year']}"
    assert result[2]["month"] == 3, f"Expected month 3, got {result[2]['month']}"
    assert result[2]["day"] == 25, f"Expected day 25, got {result[2]['day']}"
    print("  ✓ Row 3 partitions correct: 2024-03-25")

    # Check row count
    assert result_df.count() == 3, f"Expected 3 rows, got {result_df.count()}"
    print("  ✓ Row count correct: 3 rows")

    # Check metadata columns preserved
    assert result[0]["_source"] == "test", "Metadata _source not preserved"
    assert result[0]["_endpoint"] == "/test", "Metadata _endpoint not preserved"
    print("  ✓ Metadata columns preserved")

    # Test 2: Append mode
    print("  Testing append mode...")
    additional_data = pd.DataFrame({
        "Interval_Start": [datetime(2024, 1, 15, 11, 0)],
        "value": [150.0],
        "_ingested_at": [datetime.utcnow()],
        "_source": ["test"],
        "_endpoint": ["/test"]
    })

    write_to_bronze(
        df=additional_data,
        table_name=test_table_name,
        partition_cols=["year", "month", "day"],
        mode="append"
    )

    result_df = spark.table(full_table_name)
    assert result_df.count() == 4, f"Expected 4 rows after append, got {result_df.count()}"
    print("  ✓ Append mode works: 4 rows total")

    # Cleanup
    spark.sql(f"DROP TABLE {full_table_name}")
    print(f"  ✓ Cleaned up test table")

    print("\n✅ Test 3 PASSED: write_to_bronze() works correctly\n")
    return True

# Run the test
try:
    test_write_to_bronze()
except AssertionError as e:
    print(f"\n❌ Test 3 FAILED: {e}\n")
except Exception as e:
    print(f"\n❌ Test 3 ERROR: {e}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: run_bronze_quality_checks()
# MAGIC
# MAGIC Tests data quality validation functions.

# COMMAND ----------

def test_quality_checks():
    """Test run_bronze_quality_checks() detects data quality issues"""
    print("Running Test 4: run_bronze_quality_checks()")
    print("-" * 60)

    test_table_name = "test_quality_checks"
    full_table_name = f"{TEST_CATALOG}.{TEST_SCHEMA_BRONZE}.{test_table_name}"

    # Clean up if exists from previous run
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    except:
        pass

    # Test 1: Table doesn't exist
    print("  Testing with non-existent table...")
    checks = run_bronze_quality_checks(test_table_name)
    assert checks.get("status") == "table_not_found", "Should return table_not_found status"
    print("  ✓ Non-existent table returns table_not_found status")

    # Test 2: Table with quality issues
    print("  Creating table with known quality issues...")
    test_df = spark.createDataFrame([
        (datetime(2024, 1, 1, 0, 0), 100.0, datetime.utcnow()),  # Good row
        (datetime(2024, 1, 1, 1, 0), 200.0, datetime.utcnow()),  # Good row
        (datetime(2024, 1, 1, 1, 0), 150.0, datetime.utcnow()),  # Duplicate interval
        (datetime(2024, 1, 2, 0, 0), 300.0, datetime.utcnow()),  # Good row
        (None, 400.0, datetime.utcnow()),                        # Null interval
        (datetime(2024, 1, 3, 0, 0), 500.0, None),               # Null _ingested_at
    ], ["Interval_Start", "value", "_ingested_at"])

    test_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
    print("  ✓ Created table with 6 rows (including quality issues)")

    # Run quality checks
    print("  Running quality checks...")
    checks = run_bronze_quality_checks(test_table_name)

    # Verify results
    print(f"  Checking row count...")
    assert checks["row_count"] == 6, f"Expected 6 rows, got {checks['row_count']}"
    print(f"  ✓ Row count correct: {checks['row_count']}")

    print(f"  Checking for null timestamps...")
    assert checks["null_time_column"] == 1, f"Expected 1 null timestamp, got {checks['null_time_column']}"
    print(f"  ✓ Null timestamp detected: {checks['null_time_column']}")

    print(f"  Checking for null _ingested_at...")
    assert checks["null_ingested_at"] == 1, f"Expected 1 null _ingested_at, got {checks['null_ingested_at']}"
    print(f"  ✓ Null _ingested_at detected: {checks['null_ingested_at']}")

    print(f"  Checking for duplicate intervals...")
    assert checks["duplicate_intervals"] == 1, f"Expected 1 duplicate interval, got {checks['duplicate_intervals']}"
    print(f"  ✓ Duplicate interval detected: {checks['duplicate_intervals']}")

    print(f"  Checking date range...")
    assert checks["min_date"] == "2024-01-01", f"Expected min_date 2024-01-01, got {checks['min_date']}"
    assert checks["max_date"] == "2024-01-03", f"Expected max_date 2024-01-03, got {checks['max_date']}"
    print(f"  ✓ Date range correct: {checks['min_date']} to {checks['max_date']}")

    # Test 3: Clean data (no quality issues)
    print("  Testing with clean data...")
    clean_df = spark.createDataFrame([
        (datetime(2024, 1, 1, 0, 0), 100.0, datetime.utcnow()),
        (datetime(2024, 1, 1, 1, 0), 200.0, datetime.utcnow()),
        (datetime(2024, 1, 1, 2, 0), 300.0, datetime.utcnow()),
    ], ["Interval_Start", "value", "_ingested_at"])

    clean_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    checks = run_bronze_quality_checks(test_table_name)
    assert checks["row_count"] == 3, f"Expected 3 rows"
    assert checks["null_time_column"] == 0, f"Expected 0 null timestamps"
    assert checks["null_ingested_at"] == 0, f"Expected 0 null _ingested_at"
    assert checks["duplicate_intervals"] == 0, f"Expected 0 duplicates"
    print("  ✓ Clean data passes all quality checks")

    # Test 4: Empty table
    print("  Testing with empty table...")
    empty_df = spark.createDataFrame([], "Interval_Start TIMESTAMP, value DOUBLE, _ingested_at TIMESTAMP")
    empty_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    checks = run_bronze_quality_checks(test_table_name)
    assert checks["row_count"] == 0, "Expected 0 rows"
    print("  ✓ Empty table handled correctly")

    # Cleanup
    spark.sql(f"DROP TABLE {full_table_name}")
    print(f"  ✓ Cleaned up test table")

    print("\n✅ Test 4 PASSED: run_bronze_quality_checks() works correctly\n")
    return True

# Run the test
try:
    test_quality_checks()
except AssertionError as e:
    print(f"\n❌ Test 4 FAILED: {e}\n")
except Exception as e:
    print(f"\n❌ Test 4 ERROR: {e}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER TEST SUMMARY")
print("=" * 60)
print()
print("All 4 critical tests completed!")
print()
print("Tests run:")
print("  1. ✅ table_exists() - Table detection")
print("  2. ✅ determine_load_dates() - Incremental load logic")
print("  3. ✅ write_to_bronze() - Data writing and partitioning")
print("  4. ✅ run_bronze_quality_checks() - Data quality validation")
print()
print("=" * 60)
print()
print("Next steps:")
print("  - Review any failed tests above")
print("  - Test catalog can be dropped with:")
print(f"    DROP CATALOG IF EXISTS {TEST_CATALOG} CASCADE;")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment and run to remove test catalog.

# COMMAND ----------

# Uncomment to clean up test catalog
# spark.sql(f"DROP CATALOG IF EXISTS {TEST_CATALOG} CASCADE")
# print(f"✓ Dropped test catalog: {TEST_CATALOG}")
