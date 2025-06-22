# FedScope Employment Data

This repository contains **140+ million federal employee records** from 1998-2024, processed from the official FedScope Employment Cube datasets. 

## Quick Start: Download the Processed Data

**📊 Get the cleaned data:** The processed data is available as 72 quarterly Parquet files directly in this repository at `fedscope_data/parquet/`.

Each file contains 1.7-2.3 million employee records with demographics, job details, and compensation data.

```python
import pandas as pd
import glob

# Load a single quarter
df = pd.read_parquet('fedscope_data/parquet/fedscope_employment_September_2024.parquet')

# Load all quarters
parquet_files = glob.glob('fedscope_data/parquet/*.parquet')
all_data = pd.concat([pd.read_parquet(f) for f in parquet_files])
```

## Alternative: Process the Raw Data Yourself

This repository also contains the **processing pipeline** and **raw ZIP files** if you want to recreate the dataset or modify the processing:

- **Original ZIP files**: All 72 quarterly FedScope Employment Cube ZIP files are included in `fedscope_data/raw/`
- **Processed Parquet files**: 72 quarterly files with all lookups merged in `fedscope_data/parquet/` (~2.3 GB total)
- **Processing scripts**: Extract, clean, and merge the lookup tables
- **Documentation**: Original OPM documentation PDFs

See the "Recreating the Dataset" section below for detailed processing instructions.

## What's in This Dataset

- **Time Range**: March 1998 through September 2024 (72 quarters)
- **Coverage**: All federal civilian employees (excludes military, postal, intelligence)
- **Records**: 140+ million individual employee snapshots
- **Fields**: 42 columns including demographics, job details, and compensation
- **Format**: Parquet files with built-in compression and fast read performance

## Key Features

- **Complete Time Series**: Track federal workforce changes over 26+ years
- **Denormalized Data**: All lookup codes replaced with human-readable descriptions
- **Privacy Protected**: No individual identification possible (aggregated snapshots)
- **Analysis Ready**: Clean, consistent format across all quarters
- **Efficient Storage**: Parquet format with ~85% compression vs equivalent CSV

## Field Overview

Each employee record contains:

- **When & Where**: Year, quarter, agency, location
- **Demographics**: Age group, education level, length of service
- **Job Details**: Occupation, grade, supervisory status, work schedule
- **Compensation**: Salary, salary range (some redacted for privacy)

## Example Analysis

```python
import pandas as pd

# Load a quarter of data
df = pd.read_parquet('fedscope_data/parquet/fedscope_employment_September_2024.parquet')

# Count employees by agency
agency_counts = df.groupby('agysubt')['employment'].sum().sort_values(ascending=False)
print(agency_counts.head())

# Average salary by education level (excluding redacted values)
salary_by_education = df[df['salary'].notna()].groupby('edlvlt')['salary'].mean()
```

## Repository Contents

- **`fedscope_data/parquet/`** - 72 quarterly Parquet files (ready to use!)
- **`fedscope_data/raw/`** - Original ZIP files from OPM
- **Processing pipeline** - Scripts to recreate the Parquet files from ZIP files
- **Documentation** - Field guides and original OPM documentation

## Data Sources

- **Source**: U.S. Office of Personnel Management (OPM) FedScope Employment Cube
- **Official Site**: https://www.fedscope.opm.gov/
- **License**: Public domain (U.S. Government work)

## Need Help?

- **Understanding the data**: Check our [field documentation](https://abigailhaddad.github.io/fedscope_employment/)
- **Processing questions**: See the technical sections below
- **Issues**: [Report on GitHub](https://github.com/abigailhaddad/fedscope_employment/issues)

---

## Data Quality

The processing pipeline handles the following data quality issue:

- **Duplicate Lookup Entries**: Early years (1998-2003) contain duplicate agency entries with same codes but different names. The pipeline uses the first occurrence.

## Data Schema

### Field Format Structure

The processed dataset contains three types of fields:

1. **Code Fields**: Original FedScope codes used for categorization and joining (e.g., `agelvl`, `edlvl`, `occ`)
2. **Description Fields**: Human-readable labels derived from lookup tables (e.g., `agelvlt`, `edlvlt`, `occt`)
3. **Data Fields**: Actual analytical values (e.g., `employment`, `salary`, `year`)

**Naming Pattern**: Description fields follow the pattern of adding 't' to the code field name (e.g., `agelvl` → `agelvlt`).

The dataset includes for each employee record:

- **Time**: Year, quarter, dataset key
- **Demographics**: Age level code/description, education level code/description
- **Job Characteristics**: Occupation codes/descriptions, PATCO category codes/descriptions, pay plan/grade codes/descriptions, GS equivalent grade codes/descriptions
- **Compensation**: Salary (null for redacted values), salary level codes/descriptions
- **Work Details**: Schedule codes/descriptions, status codes/descriptions, appointment type codes/descriptions, supervisory status codes/descriptions
- **Organization**: Sub-agency codes/descriptions, agency codes, location codes/descriptions
- **Other**: STEM occupation indicator codes/descriptions, length of service, employment count

**For Analysis**: Use the description fields (ending in 't') which provide human-readable values. Code fields are primarily for data processing.

## Data Coverage

- **1998-2008**: September only (annual snapshots)
- **2009**: September, December  
- **2010-2024**: Full quarterly coverage (March, June, September, December)
- **2024**: Through September

## Advanced Usage

### Working with Large Datasets

For analyzing large portions of the dataset efficiently, consider:

```python
import pandas as pd
import glob

# Read multiple quarters efficiently
parquet_files = glob.glob('fedscope_data/parquet/fedscope_employment_*_2024.parquet')
recent_data = pd.concat([pd.read_parquet(f) for f in parquet_files])

# Filter and analyze
agency_summary = recent_data.groupby('agysubt').agg({
    'employment': 'sum',
    'salary': ['mean', 'median', 'count']
}).round(0)

# Time series analysis
quarterly_trends = recent_data.groupby(['year', 'quarter'])['employment'].sum()
```

## Recreating the Dataset

If you want to recreate the Parquet files from scratch:

### Prerequisites
1. The 72 quarterly ZIP files are already included in this repository in the `fedscope_data/raw/` directory. No need to download them separately!
   
   *(Alternative: If you prefer to download from opm directly, get all 72 quarterly **FedScope Employment Cube** ZIP files from https://www.opm.gov/data/datasets/ and place them in `fedscope_data/raw/`)*

2. Install dependencies:
```bash
pip install pandas pyarrow
```

### Pipeline Steps
```bash
# Default: Extract ZIP files and create Parquet files
python main.py

# Or individual steps:
python main.py --extract          # Extract ZIP files only
python main.py --parquet          # Create Parquet files only
python main.py --validate         # Validate Parquet files only
```

### What the Pipeline Does

Running `python main.py` will:

1. **Extract** all 72 ZIP files from `fedscope_data/raw/` to `fedscope_data/extracted/`
2. **Parse** the fact data files (FACTDATA_*.TXT) and lookup tables (DT*.txt) from each quarter
3. **Merge** the lookup tables with the fact data, replacing codes with human-readable descriptions
4. **Export** each quarter as a separate Parquet file in `fedscope_data/parquet/`
5. **Validate** that all files were created correctly with proper lookup joins

### Architecture
```
72 ZIP files → Extract → Parse fact & lookup tables → Join lookups → 72 Parquet files
```

### Output Files
- `fedscope_data/parquet/fedscope_employment_{Quarter}_{Year}.parquet` - 72 quarterly files (~30MB each)
- `fedscope_data/extracted/` - Extracted text files (can be deleted after processing)

### File Sizes
- **ZIP files**: ~300MB total
- **Extracted TXT files**: ~3GB total 
- **Parquet files**: ~2.3GB total (much more efficient than CSV)

## Repository Structure

```
fedscope_employment/
├── fedscope_data/
│   ├── raw/                    # Contains all 72 quarterly ZIP files
│   ├── extracted/              # Extracted data files (created by pipeline)
│   └── parquet/                # 72 quarterly Parquet files (final output)
├── main.py                     # Main orchestration script
├── rename_and_extract.py       # Identifies, renames, and extracts ZIP files
├── text_to_parquet.py          # Converts TXT files to Parquet with lookups
├── validate_parquet.py         # Validates Parquet files
└── documentation_pdfs/         # PDF documentation for each quarterly dataset
```

## Data Structure

Each quarterly dataset contains:
- **FACTDATA_\*.TXT**: Main fact table with employee records (1.7M - 2.2M records per quarter)
- **DT\*.txt**: Lookup tables providing descriptions for coded values
  - DTagelvl.txt - Age levels
  - DTagy.txt - Agencies  
  - DTedlvl.txt - Education levels
  - DTgsegrd.txt - General Schedule grades
  - DTloc.txt - Locations
  - DTocc.txt - Occupations
  - DTpatco.txt - PATCO categories
  - DTppgrd.txt - Pay plans and grades
  - DTsallvl.txt - Salary levels
  - DTstemocc.txt - STEM occupations
  - DTsuper.txt - Supervisory status
  - DTtoa.txt - Types of appointment
  - DTwrksch.txt - Work schedules
  - DTwkstat.txt - Work status

## Data Preparation

The FedScope files from OPM come with UUID-style names (e.g., `9f7eab79-e539-45fe-8e8a-00663e3ec190`) rather than descriptive names. The `rename_and_extract.py` script:

1. Identifies the time period by examining the FACTDATA filename inside each ZIP
2. Renames files to a standard format: `FedScope_Employment_{Quarter}_{Year}.zip`
3. Extracts them to matching directory names

## Performance Notes

- **Parquet format**: Fast to read with built-in compression
- **Columnar storage**: Efficient for analytics workloads
- **File size**: ~2.3GB total (highly compressed)
- **Memory usage**: Can read individual quarters (~30MB) or use query engines for out-of-core processing

---

*This is an independent data processing project. For official federal employment statistics, visit [fedscope.opm.gov](https://www.fedscope.opm.gov/).*