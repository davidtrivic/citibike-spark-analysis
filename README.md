# Citi Bike Ride Data Analysis (PySpark DataFrame API)

An end-to-end data analysis pipeline over the **Citi Bike** trip dataset (October 2024), built entirely with the **Spark DataFrame API**, combined with hourly weather data to study how conditions like precipitation and temperature affect ridership. Built as a university project for the "Selected Topics in Operating Systems" course.

## Overview

The pipeline loads raw Citi Bike trip records, cleans and enriches them with derived features, and runs a series of aggregate analyses - active stations, common routes, round-trip behavior, and the effect of weather (precipitation, temperature bands) on ride volume and duration. All transformations use the **DataFrame API exclusively**; no significant data is pulled to the driver via `.collect()`, aside from small aggregated results.

## Pipeline Stages

1. **Load** - read the raw CSV trip data with an inferred schema
2. **Clean** (`clear_df`) - drop invalid trips (`started_at >= ended_at`), drop nulls, and remove statistical outliers in trip duration using a 3-standard-deviation bound
3. **Feature engineering** (`add_cols`) - derive:
   - `ride_duration` (seconds)
   - `hour_of_day`, `day_of_week`
   - `is_weekend` indicator
   - `round_trip` indicator (same start/end station)
   - `distance` - Euclidean approximation from start/end coordinates
4. **Ride pattern analysis** (`analyze_rides`) - ride counts and average duration, grouped by weekday/weekend, membership type, and hour of day
5. **Most active stations** (`most_active_stations`) - top-N stations by ride volume (with a minimum ride threshold for stability), enriched with member ratio, round-trip ratio, average duration, and ride distribution across night/morning/afternoon/evening
6. **Most common routes** (`most_common_routes`) - top-N start→end station pairs (with a minimum ride threshold), with average duration and duration variability (standard deviation)
7. **Round-trip analysis** (`analyze_round_trips`) - round-trip share and average duration, broken down by membership type and bike type
8. **Weather join** - Citi Bike timestamps are converted to UTC and joined with Meteostat hourly weather data (station `KJRB0`) on date + hour
9. **Weather cleaning** (`clear_meteostat_df`) - restrict to October, derive a precipitation indicator and 5-level temperature band (`< 5°C`, `5–10°C`, `10–15°C`, `15–20°C`, `≥ 20°C`)
10. **Combined analysis**:
    - `analyze_combined_df` - ride volume and average duration by weekend/weekday and precipitation
    - `analyze_temp_bands` - ride volume and average duration by temperature band and membership type

Every intermediate stage is written out to disk (as CSV, capped at the first 50 rows for inspection purposes) so results can be inspected step by step.

## Tech Stack

- **Python 3**
- **PySpark** (DataFrame API - `pyspark.sql.functions`)

## Project Structure

```
.
└── start.py    # full pipeline: cleaning, feature engineering, analysis, weather join
```

Expected data layout (not included in this repo - see [Data](#data) below):

```
projekat2_data/
├── citibike/           # raw Citi Bike trip CSVs
└── KJRB0.csv.gz        # Meteostat hourly weather data for station KJRB0
```

## Getting Started

### Prerequisites
- Python 3.8+
- PySpark (`pip install pyspark`)

### Run

```bash
python start.py
```

Output is written to a set of numbered folders (`1-df`, `2-clear_df`, `3-extended_df`, … `11-analyze_temp_bands`), each corresponding to one stage of the pipeline described above.

## Data

This project uses the public **Citi Bike** trip history dataset and **Meteostat** hourly weather data for station `KJRB0`. Raw data files are not included in this repository due to size - download them separately and place them under `projekat2_data/` as shown above before running the script.
