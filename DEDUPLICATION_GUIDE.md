# Bronze Layer Deduplication Guide

## Problem

The bronze layer was experiencing duplicate data due to the 7-day lookback window in incremental loads combined with append-only writes. This caused overlapping date ranges to be loaded multiple times, resulting in duplicate records.

## Solution Implemented

### 1. MERGE Logic for Incremental Loads

The `write_to_bronze()` function in `01_bronze_ingestion.py` now uses Delta Lake MERGE operations instead of simple appends. This ensures that:

- **New records** are inserted
- **Existing records** (based on business keys) are updated with the latest data
- **No duplicates** accumulate over time

### Business Keys Configured

Each data source has specific business keys that uniquely identify a record:

| Table Name | Business Keys |
|------------|--------------|
| `lmp_day_ahead_market` | `Interval_Start`, `Location`, `Market`, `Location_Type` |
| `lmp_by_settlement_point` | `Interval_Start`, `Settlement_Point` |
| `solar_hourly_actual_forecast` | `Interval_Start` |
| `solar_hourly_actual_forecast_regional` | `Interval_Start`, `Geographic_Region` |
| `wind_hourly_actual_forecast` | `Interval_Start` |
| `wind_hourly_actual_forecast_regional` | `Interval_Start`, `Geographic_Region` |
| `load_forecast_7day` | `Time`, `PublishDate`, `WeatherZone` |
| `fuel_mix` | `Time` |

### How MERGE Works

```python
MERGE INTO bronze.lmp_day_ahead_market AS target
USING new_data AS source
ON target.Interval_Start = source.Interval_Start
   AND target.Location = source.Location
   AND target.Market = source.Market
   AND target.Location_Type = source.Location_Type
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```

This ensures that:
- If a record with the same business keys exists, it gets **updated** with the latest values
- If it's a new record, it gets **inserted**
- No duplicates are created

## Cleaning Up Existing Duplicates

If you already have duplicates in your bronze tables, use the deduplication utilities:

### Option 1: Deduplicate LMP Day-Ahead Market

```python
# In Databricks notebook or the bronze ingestion notebook
deduplicate_lmp_dam()
```

This will:
1. Identify all duplicate records based on business keys
2. Keep only the **most recent** record (based on `_ingested_at` timestamp)
3. Create a **backup table** before making changes
4. Overwrite the original table with deduplicated data

### Option 2: Deduplicate Any Bronze Table

```python
# For other tables
deduplicate_bronze_table(
    table_name="lmp_by_settlement_point",
    business_keys=["Interval_Start", "Settlement_Point"]
)
```

### Example Output

```
Deduplicating ercot_energy.bronze.lmp_day_ahead_market...
  Business keys: ['Interval_Start', 'Location', 'Market', 'Location_Type']
  Rows before deduplication: 1,250,000
  Rows after deduplication: 1,000,000
  Duplicates removed: 250,000
  Creating backup: ercot_energy.bronze.lmp_day_ahead_market_backup_20260113_143022
  Overwriting ercot_energy.bronze.lmp_day_ahead_market with deduplicated data...
✓ Successfully deduplicated ercot_energy.bronze.lmp_day_ahead_market
  Backup saved to: ercot_energy.bronze.lmp_day_ahead_market_backup_20260113_143022
```

## Verification

After deduplication or running a new load with MERGE logic, verify no duplicates exist:

```sql
-- Check for duplicates in LMP Day-Ahead Market
SELECT
    Interval_Start, Location, Market, Location_Type,
    COUNT(*) as duplicate_count
FROM ercot_energy.bronze.lmp_day_ahead_market
GROUP BY Interval_Start, Location, Market, Location_Type
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
```

If this query returns zero rows, you have no duplicates! 🎉

## Impact on Downstream Layers

### Silver Layer
The silver layer (`02_silver_transformations.py`) should be re-run after deduplicating bronze to ensure clean data propagates through:

```python
# Re-run silver transformations
dbutils.notebook.run("02_silver_transformations", timeout_seconds=3600)
```

### Gold Layer
Similarly, re-run gold aggregations:

```python
# Re-run gold aggregations
dbutils.notebook.run("03_gold_aggregations", timeout_seconds=3600)
```

## Future Loads

All future incremental loads will automatically use MERGE logic, preventing new duplicates from being created. The 7-day lookback window will continue to handle late-arriving data without creating duplicates.

## Technical Details

### Why Did Duplicates Occur?

1. **7-Day Lookback**: The incremental load logic goes back 7 days to catch late-arriving data
2. **Append Mode**: Previous implementation used `.mode("append")` which just adds rows
3. **Overlapping Windows**: Each daily run would re-load the previous 7 days worth of data

Example timeline:
- **Jan 10 run**: Loads Jan 3-10 data ✓
- **Jan 11 run**: Loads Jan 4-11 data → Jan 4-10 now **duplicated**
- **Jan 12 run**: Loads Jan 5-12 data → Jan 5-11 now **duplicated**

### Why MERGE Solves This

MERGE operations check if a record already exists (based on business keys) before inserting. This makes the operation **idempotent** - you can run it multiple times with the same data and get the same result.

## Rollback

If you need to rollback after deduplication, the original data is preserved in a backup table:

```sql
-- List backup tables
SHOW TABLES IN ercot_energy.bronze LIKE '*_backup_*';

-- Restore from backup
CREATE OR REPLACE TABLE ercot_energy.bronze.lmp_day_ahead_market AS
SELECT * FROM ercot_energy.bronze.lmp_day_ahead_market_backup_20260113_143022;
```

## Questions?

For issues or questions about deduplication:
1. Check the backup table exists before deduplication
2. Verify business keys match your data structure
3. Review the bronze ingestion logs for MERGE operation details
