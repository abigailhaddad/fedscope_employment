# FedScope Employment Data Loader

This repository contains scripts to load FedScope Employment data into a DuckDB database.

## Overview

FedScope Employment data provides quarterly snapshots of federal civilian employment. This project:
- Processes downloaded FedScope data files
- Extracts and loads data into DuckDB
- Preserves all lookup tables with dataset keys for accurate historical analysis

## Files

- `fix_and_extract.py` - Identifies and extracts UUID-named zip files
- `load_to_duckdb_concat.py` - Loads all data into DuckDB with concatenated lookup tables
- `requirements.txt` - Python dependencies

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Place downloaded FedScope zip files in `fedscope_data/` directory

3. Fix and extract any UUID-named files:
   ```bash
   python fix_and_extract.py
   ```

4. Load data into DuckDB:
   ```bash
   python load_to_duckdb_concat.py
   ```

## Database Structure

The DuckDB database (`fedscope_employment_concat.duckdb`) contains:

### Fact Table
- `employment_facts` - Main employment records with dataset_key for joining

### Lookup Tables
All lookup tables include a `dataset_key` field (format: `YYYY_QQ`) to maintain historical accuracy:
- `lookup_agency` - Agency codes and descriptions
- `lookup_occupation` - Occupation codes and descriptions  
- `lookup_location` - Location codes and descriptions
- `lookup_education` - Education level codes
- `lookup_salary_level` - Salary level bands
- And more...

### Key Design Decision

Each dataset's lookup tables are preserved separately using a composite key of (dataset_key, code). This ensures:
- No duplicate key conflicts
- Historical accuracy when codes change meaning over time
- Easy joining between fact data and corresponding lookups

## Example Query

```sql
-- Get employment data with decoded values for 2021 Q3
SELECT 
    f.employment,
    f.salary,
    a.agysub_desc as agency,
    o.occ_desc as occupation,
    l.loc_desc as location
FROM employment_facts f
LEFT JOIN lookup_agency a ON f.dataset_key = a.dataset_key AND f.agysub_code = a.agysub_code
LEFT JOIN lookup_occupation o ON f.dataset_key = o.dataset_key AND f.occ_code = o.occ_code
LEFT JOIN lookup_location l ON f.dataset_key = l.dataset_key AND f.loc_code = l.loc_code
WHERE f.year = 2021 AND f.quarter = 'Q3'
LIMIT 10;
```