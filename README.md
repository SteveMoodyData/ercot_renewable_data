# ERCOT Renewable Energy Data Pipeline

A Databricks medallion architecture pipeline for ingesting and analyzing ERCOT renewable energy data using the `gridstatus` Python library.

## Overview

This pipeline extracts solar and wind generation data (actuals and forecasts) from ERCOT's Public API, transforms it through bronze → silver → gold layers, and produces analytics-ready datasets for renewable energy forecasting.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ERCOT Public API                                   │
│  (https://api.ercot.com)                                                    │
│  - Solar hourly actual/forecast                                             │
│  - Wind hourly actual/forecast                                              │
│  - Regional breakdowns                                                       │
│  - Load forecasts                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (01_bronze_ingestion.py)                                      │
│  ────────────────────────────────────                                       │
│  • Raw API responses stored as Delta tables                                 │
│  • Metadata columns: _ingested_at, _source, _endpoint                       │
│  • Partitioned by year/month/day                                            │
│  • Incremental load with configurable lookback                              │
│                                                                             │
│  Tables:                                                                    │
│  - solar_hourly_actual_forecast                                             │
│  - solar_hourly_actual_forecast_regional                                    │
│  - wind_hourly_actual_forecast                                              │
│  - wind_hourly_actual_forecast_regional                                     │
│  - fuel_mix                                                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (02_silver_transformations.py)                                │
│  ────────────────────────────────────────────                               │
│  • Standardized column names (snake_case)                                   │
│  • Calculated fields: forecast error, curtailment estimates                 │
│  • Time-based features: hour_of_day, day_of_week, is_weekend               │
│  • Combined renewable generation views                                      │
│                                                                             │
│  Tables:                                                                    │
│  - solar_hourly                                                             │
│  - wind_hourly                                                              │
│  - solar_hourly_regional                                                    │
│  - wind_hourly_regional                                                     │
│  - renewable_generation_combined                                            │
│  - renewable_generation_totals                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (03_gold_aggregations.py)                                       │
│  ────────────────────────────────────                                       │
│  • Forecast accuracy metrics (MAE, MAPE, RMSE, bias)                        │
│  • Generation patterns (hourly, daily, monthly)                             │
│  • Curtailment analysis                                                     │
│  • ML feature store for forecasting models                                  │
│                                                                             │
│  Tables:                                                                    │
│  - solar_forecast_accuracy_daily                                            │
│  - solar_forecast_accuracy_by_hour                                          │
│  - wind_forecast_accuracy_daily                                             │
│  - wind_forecast_accuracy_by_hour                                           │
│  - generation_hourly_patterns                                               │
│  - generation_monthly_summary                                               │
│  - generation_dow_patterns                                                  │
│  - curtailment_hourly_patterns                                              │
│  - curtailment_daily_summary                                                │
│  - solar_forecast_features                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

### 1. ERCOT API Registration

1. Go to https://apiexplorer.ercot.com/
2. Create an account
3. Subscribe to the Public API
4. Note your:
   - Username
   - Password
   - Subscription Key

### 2. Databricks Environment

- Databricks workspace with Unity Catalog enabled
- Cluster with Python 3.11+
- Access to create catalogs and schemas

### 3. Configure Secrets

```bash
# Using Databricks CLI
databricks secrets create-scope ercot

databricks secrets put-secret ercot api_username --string-value "your_username"
databricks secrets put-secret ercot api_password --string-value "your_password"  
databricks secrets put-secret ercot subscription_key --string-value "your_subscription_key"
```

## Quick Start

### 1. Run Setup
```
Run: 00_setup.py
```
Creates the `ercot_energy` catalog with bronze/silver/gold schemas.

### 2. Initial Data Load
```
Run: 01_bronze_ingestion.py
Parameters:
  - load_type: full
  - sources: solar_hourly,wind_hourly
```

### 3. Transform Data
```
Run: 02_silver_transformations.py
Run: 03_gold_aggregations.py
```

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `00_setup.py` | One-time setup of catalog, schemas, and volumes |
| `01_bronze_ingestion.py` | Extract data from ERCOT API to bronze tables |
| `02_silver_transformations.py` | Clean and standardize data |
| `03_gold_aggregations.py` | Create analytics and ML feature tables |
| `test_01_bronze_ingestion.py` | Integration tests for bronze layer functions |

## Testing

### Test Coverage

The project includes integration tests for the bronze layer that validate critical data ingestion functions. Tests are designed to run manually in Databricks using an isolated test catalog.

**Test Notebook**: `test_01_bronze_ingestion.py`

### What's Tested

The bronze layer tests cover these critical functions:

1. **`table_exists()`** - Table detection logic
   - Validates correct detection of existing tables
   - Handles non-existent tables appropriately
   - Tests catalog and schema resolution

2. **`determine_load_dates()`** - Incremental load date calculation
   - Initial load scenario (uses default start date)
   - Incremental load scenario (calculates lookback from last load)
   - Custom lookback period handling
   - Late-arriving data support

3. **`write_to_bronze()`** - Data writing and partitioning
   - Partition column generation (year, month, day)
   - Timestamp extraction from Interval_Start/Time columns
   - Append vs overwrite modes
   - Metadata column preservation
   - Schema merging

4. **`run_bronze_quality_checks()`** - Data quality validation
   - Row count verification
   - Null value detection (timestamps, metadata)
   - Duplicate interval identification
   - Date range validation
   - Empty table handling

### Running Tests

**Prerequisites:**
- Databricks workspace with Unity Catalog enabled
- Cluster attached to workspace
- No special permissions required (test catalog is isolated)

**Steps:**

1. Upload `test_01_bronze_ingestion.py` to your Databricks workspace
2. Attach the notebook to any cluster
3. Run all cells

The tests will:
- Create an isolated test catalog: `ercot_energy_test`
- Execute all 4 test suites
- Print detailed results for each test (✅ pass / ❌ fail)
- Clean up test tables automatically after each test
- Display a summary of all test results

**Test Output Example:**
```
Running Test 1: table_exists()
------------------------------------------------------------
  ✓ Non-existent table correctly returns False
  ✓ Created test table: ercot_energy_test.bronze.test_exists_table
  ✓ Existing table correctly returns True
  ✓ Cleaned up test table

✅ Test 1 PASSED: table_exists() works correctly
```

### Test Environment

- **Test Catalog**: `ercot_energy_test`
- **Test Schema**: `bronze`
- **Isolation**: Tests never touch production or dev tables
- **Cleanup**: Optional command to drop entire test catalog when done

To remove the test catalog after testing:
```sql
DROP CATALOG IF EXISTS ercot_energy_test CASCADE;
```

### Test Approach

These are **integration tests** that:
- Run in actual Databricks environment (not unit tests)
- Use real Spark DataFrames and Delta tables
- Test against Unity Catalog
- Validate end-to-end functionality
- Can be run manually as needed (not automated CI/CD)

This approach is suitable for POC/development environments where manual validation is sufficient.

## Data Sources

### Solar Data
- **Endpoint**: `/np4-737-cd/spp_hrly_avrg_actl_fcast`
- **Update Frequency**: Hourly
- **History**: 48-hour rolling actuals + 168-hour forward forecast
- **Key Columns**: 
  - `ACTUAL_SYSTEM_WIDE` - Actual generation (MW)
  - `STPPF_SYSTEM_WIDE` - Short-Term PV Power Forecast
  - `HSL_SYSTEM_WIDE` - High Sustained Limit (potential)

### Wind Data
- **Endpoint**: `/np4-732-cd/wpp_hrly_avrg_actl_fcast`
- **Update Frequency**: Hourly
- **Key Columns**:
  - `ACTUAL_SYSTEM_WIDE` - Actual generation (MW)
  - `STWPF_SYSTEM_WIDE` - Short-Term Wind Power Forecast
  - `HSL_SYSTEM_WIDE` - High Sustained Limit (potential)

### Regional Data
Solar and wind data are also available by geographic region (Panhandle, CIS, South, North, West, Houston).

## Key Metrics

### Forecast Accuracy
- **MAE** (Mean Absolute Error): Average forecast error in MW
- **MAPE** (Mean Absolute Percentage Error): Error as percentage of actual
- **RMSE** (Root Mean Square Error): Penalizes large errors
- **Bias**: Systematic over/under forecasting tendency

### Curtailment
Estimated curtailment = HSL (potential) - Actual (generated)

Occurs when the grid cannot absorb all available renewable energy.

## Scheduling (Databricks Workflows)

### Recommended Job Schedule

```yaml
# Bronze ingestion - hourly
- notebook: 01_bronze_ingestion
  schedule: "0 */1 * * *"  # Every hour
  parameters:
    load_type: incremental
    sources: solar_hourly,wind_hourly

# Silver transforms - every 2 hours
- notebook: 02_silver_transformations
  schedule: "0 */2 * * *"

# Gold aggregations - daily
- notebook: 03_gold_aggregations  
  schedule: "0 6 * * *"  # 6 AM daily
```

## Extending the Pipeline

### Adding New Data Sources

1. Add source definition to `DATA_SOURCES` dict in `01_bronze_ingestion.py`
2. Create ingestion function using `gridstatus` methods
3. Add silver transformation in `02_silver_transformations.py`
4. Create gold aggregations as needed

### Available gridstatus Methods for ERCOT

```python
from gridstatus.ercot_api.ercot_api import ErcotAPI

api = ErcotAPI(username, password, subscription_key)

# Solar
api.get_solar_actual_and_forecast_hourly(date, end)
api.get_solar_actual_and_forecast_by_geographical_region_hourly(date, end)

# Wind
api.get_wind_actual_and_forecast_hourly(date, end)
api.get_wind_actual_and_forecast_by_geographical_region_hourly(date, end)

# Pricing
api.get_lmp_by_bus_dam(date, end)
api.get_lmp_by_settlement_point(date, end)

# Load
api.get_load_forecast(date, end)

# Direct endpoint access
api.hit_ercot_api("/np4-737-cd/spp_hrly_avrg_actl_fcast", **params)
```

## Resources

- [ERCOT API Explorer](https://apiexplorer.ercot.com/)
- [ERCOT Developer Portal](https://developer.ercot.com/)
- [gridstatus Documentation](https://opensource.gridstatus.io/)
- [gridstatus GitHub](https://github.com/gridstatus/gridstatus)

## License

This pipeline code is provided as-is. ERCOT data usage is subject to [ERCOT Terms of Use](https://www.ercot.com/help/terms/data-portal).
