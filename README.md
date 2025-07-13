# FedScope Employment Data (NOT OFFICIAL)

This repository contains **140+ million federal employee records** from 1998-2025, plus **9.8+ million separations and accessions records** from 2005-2025, processed from the official FedScope datasets. 

**🔍 Want to see quick comparisons between September 2024 and March 2025?** See: https://fluffy-narwhal-e5f260.netlify.app/

**💻 Want to get started coding with March 2025 person-level data?** See: [analysis/employment_comparisons.ipynb](analysis/employment_comparisons.ipynb)

## Quick Start

You can use this data in two ways:

### Option 1: Direct Download (Recommended)

Download individual Parquet files directly from GitHub without cloning:

```python
import pandas as pd

# Load employment data directly from GitHub
df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/employment_cube/parquet/fedscope_employment_September_2024.parquet')

# Load separations and accessions data
sep_df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/separations_accessions/parquet/fedscope_separations_FY2020-2024.parquet')
acc_df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/separations_accessions/parquet/fedscope_accessions_FY2020-2024.parquet')

# For instance, load the latest March 2025 data (see warnings below)
df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/employment_cube/parquet/fedscope_employment_March_2025.parquet')
```

Browse available files: 
- **Employment:** [employment_cube/parquet/](https://github.com/abigailhaddad/fedscope_employment/tree/main/employment_cube/parquet) (73 quarterly snapshots)
- **Separations & Accessions:** [separations_accessions/parquet/](https://github.com/abigailhaddad/fedscope_employment/tree/main/separations_accessions/parquet) (10 multi-year files)

### Option 2: Clone Repository

⚠️ **Large Repository Warning**: This repo is ~3.9GB due to the included data files. 

```bash
git clone https://github.com/abigailhaddad/fedscope_employment.git
cd fedscope_employment
```

Then load files locally:

```python
import pandas as pd

# Load employment data (recommended - see examples.py for comprehensive usage)
df = pd.read_parquet('employment_cube/parquet/fedscope_employment_September_2024.parquet')

# Load separations and accessions data  
sep_df = pd.read_parquet('separations_accessions/parquet/fedscope_separations_FY2020-2024.parquet')
acc_df = pd.read_parquet('separations_accessions/parquet/fedscope_accessions_FY2020-2024.parquet')
```

## What's Included

### Employment Cube Data
- **73 quarterly snapshots** from March 1998 through March 2025
- **1.7-2.3 million employees** per quarter 
- **52 fields** including demographics, job details, and compensation
- **Lookup tables joined** for easier usage

### Separations & Accessions Data (NEW!)
- **9.8+ million individual records** from FY2005-2025
- **5.3 million accessions** (new hires) and **4.6 million separations** (departures)
- **Same 52 fields** as employment data including demographics, job details, and compensation
- **Lookup tables pre-joined** with human-readable agency names, job titles, locations
- **Coverage:** FY2005-2025 with gaps (missing Oct-Dec 2023 and Jan-Mar 2024)
- **Note:** Earlier separations/accessions data (pre-2005) is available from [OPM directly](https://www.opm.gov/data/datasets/) but not included in this repository

## ⚠️ March 2025 Data Warnings

The March 2025 dataset has several important differences from historical data:

- **Preliminary data**: This is preliminary and subject to revision
- **Includes employees on leave**: Data includes federal employees on various types of leave who may not be currently working
- **Format differences**: Raw data structure and field names differ from historical formats (processed to match historical schema)

**Increased redaction: data suppression policy**: REDACTED values occur in fields where data suppression is required due to OPM's Data Release Policy (https://www.fedscope.opm.gov/download_Data%20Release%20Policy.pdf). This includes categorizing some Federal employees with duty locations in Maryland, Virginia, and West Virginia under the District of Columbia state category.

## Example Usage

**🚀 Quick Start: Run [examples.py](examples.py)** for comprehensive usage examples! 
- Output is saved to [examples_output.txt](examples_output.txt)
- Includes DuckDB examples for querying multiple years at once

```python
# Count employees by agency (employment is stored as strings)
agency_counts = df.groupby('agysubt')['employment'].apply(lambda x: sum(int(i) for i in x)).sort_values(ascending=False).head(10)

# Average salary by education level (convert salary to numeric, handling edge cases)
df['salary_numeric'] = df['salary'].apply(lambda x: int(float(x)) if x not in [None, 'nan', '*****', ''] and pd.notna(x) else None)
df_with_salary = df[df['salary_numeric'].notna()]
salary_by_edu = df_with_salary.groupby('edlvlt')['salary_numeric'].mean().sort_values(ascending=False)

# Track workforce over time
quarterly = df.groupby(['year', 'quarter'])['employment'].apply(lambda x: sum(int(i) for i in x))
```

### Using DuckDB for Multi-Year Analysis

```python
import duckdb

# Create a view from multiple Parquet files
con = duckdb.connect('fedscope.duckdb')
con.execute("""
    CREATE VIEW employment AS 
    SELECT * FROM read_parquet('fedscope_employment_September_2024.parquet')
    UNION ALL
    SELECT * FROM read_parquet('fedscope_employment_September_2023.parquet')
""")

# Query across years
result = con.execute("""
    SELECT year, agysubt, SUM(CAST(employment AS INTEGER)) as employees
    FROM employment
    GROUP BY year, agysubt
    ORDER BY year, employees DESC
""").fetchdf()
```

> **💡 Note:** Both datasets use string types for numeric fields like `employment` and `salary`. See [examples.py](examples.py) for proper handling.

## Repository Structure

- **`employment_cube/`** - Employment cube data (~2.2GB parquet, ~1.5GB raw)
  - `parquet/` - 73 quarterly Parquet files from 1998-2025
  - `raw/` - Original ZIP files from OPM
  - `documentation_pdfs/` - OPM documentation PDFs for each dataset
  - `lookup_duplicates_log.json` - Duplicate lookup table entries log
  - `lookup_duplicates_summary.txt` - Human-readable duplicate summary
- **`separations_accessions/`** - Separations & accessions data (~152MB parquet, ~106MB raw)
  - `parquet/` - 10 multi-year Parquet files from FY2005-2025
  - `raw/` - Original ZIP files from OPM
  - `documentation_pdfs/` - OPM documentation PDFs for each dataset
  - `lookup_duplicates_log.json` - Duplicate lookup table entries log
  - `lookup_duplicates_summary.txt` - Human-readable duplicate summary
- **`code/`** - Processing scripts and utilities
  - `fedscope_utils.py` - Shared utilities for both pipelines
  - `process_employment_cube.py` - Employment cube processing logic
  - `process_separations_accessions.py` - Separations/accessions processing logic
- **`analysis/`** - Analysis notebooks and outputs
- `process_employment_cube.py` - Employment cube processing pipeline
- `process_separations_accessions.py` - Separations/accessions processing pipeline
- `examples.py` - Comprehensive usage examples (output saved to `examples_output.txt`)
- [Additional Data Documentation](https://abigailhaddad.github.io/fedscope_employment/)

## Data Coverage

### Employment Cube (Quarterly Snapshots)
**73 quarterly snapshots total:**
- **1998-2008**: September only each year (11 files)
- **2009**: September, December  
- **2010-2023**: March, June, September, December each year (56 files)
- **2024**: March, June, September
- **2025**: March (preliminary data - see warnings above)

### Separations & Accessions (Individual Events)
**10 files total (5 separations, 5 accessions):**
- **FY2005-2009**: 5-year aggregated file
- **FY2010-2014**: 5-year aggregated file
- **FY2015-2019**: 5-year aggregated file
- **FY2020-2024**: 5-year aggregated file (missing Oct-Dec 2023 and Jan-Mar 2024)
- **April 2024-March 2025**: Most recent 12 months (labeled "March 2025")
- **Note**: Earlier data (pre-FY2005) available from OPM but not included in this repository

## Field Types

Both datasets contain the same field structure with code fields (e.g., `agelvl`) and description fields (e.g., `agelvlt`). Use the description fields ending in 't' for analysis - they contain human-readable values.

## Recreating the Datasets

### Employment Cube Data (1998-2025)
The quarterly ZIP files are included in `employment_cube/raw/`. To recreate the Parquet files:

```bash
pip install pandas pyarrow
python process_employment_cube.py
```

This runs the complete pipeline: extraction, parquet creation, and validation.

### Separations & Accessions Data (FY2005-2025)
The multi-year ZIP files are included in `separations_accessions/raw/`. To recreate the Parquet files:

```bash
python process_separations_accessions.py
```

This runs the complete pipeline: extraction, parquet creation, and validation.

### Processing Scripts
The `code/` directory contains shared utilities and core processing logic:
- `code/fedscope_utils.py` - Shared utilities (duplicate logging, lookup tables, denormalization)
- `code/process_employment_cube.py` - Employment cube processing logic
- `code/process_separations_accessions.py` - Separations/accessions processing logic

## Complete Repository Structure

```
fedscope_employment/
├── employment_cube/           # Employment snapshot data (14GB total)
│   ├── raw/                  # Original ZIP files from OPM (1.5GB)
│   ├── extracted/            # Extracted TXT files (9.8GB, gitignored) 
│   ├── parquet/              # 73 quarterly Parquet files (2.2GB)
│   ├── documentation_pdfs/   # PDF documentation for each dataset
│   ├── lookup_duplicates_log.json       # Duplicate lookup entries (JSON)
│   └── lookup_duplicates_summary.txt    # Human-readable duplicate summary
├── separations_accessions/    # Separations & accessions data (1.1GB total)
│   ├── raw/                  # Original ZIP files from OPM (106MB)
│   ├── extracted/            # Extracted TXT files (818MB, gitignored)
│   ├── parquet/              # 10 multi-year Parquet files (152MB)
│   ├── documentation_pdfs/   # PDF documentation for each dataset
│   ├── lookup_duplicates_log.json       # Duplicate lookup entries (JSON)
│   └── lookup_duplicates_summary.txt    # Human-readable duplicate summary
├── code/                     # Processing scripts and utilities
│   ├── fedscope_utils.py     # Shared utilities (duplicate logging, lookups, etc.)
│   ├── process_employment_cube.py       # Employment cube processing logic
│   └── process_separations_accessions.py # Separations/accessions processing logic
├── analysis/                 # Analysis notebooks and outputs (17MB)
│   ├── separations_analysis/ # Agency-specific turnover analysis
│   ├── inauguration_analysis/ # Presidential transition workforce analysis  
│   ├── forest_service/      # Forest Service specific analysis
│   ├── web_dashboard/       # Interactive comparison dashboard
│   └── employment_comparisons.ipynb  # Sept 2024 vs March 2025 comparison
├── process_employment_cube.py         # Employment cube processing pipeline
├── process_separations_accessions.py # Separations/accessions processing pipeline
├── examples.py              # Comprehensive usage examples
└── (documentation files)    # README.md, requirements.txt, etc.
```

## Data Structure

### Employment Cube Files
Each quarterly employment dataset contains:*
- **FACTDATA_\*.TXT**: Main fact table with employee records (1.7M - 2.2M records per quarter)
- **DT\*.txt**: Lookup tables providing descriptions for coded values
  - DTagelvl.txt - Age levels
  - DTagy.txt - Agencies  
  - DTedlvl.txt - Education levels
  - DTgsegrd.txt - General Schedule grades
  - DTloc.txt - Locations
  - DTocc.txt - Occupations
  - DTpatco.txt - PATCO categories
  - DTpp.txt - Pay plans (from 2017 onward)
  - DTppgrd.txt - Pay plans and grades
  - DTsallvl.txt - Salary levels
  - DTstemocc.txt - STEM occupations
  - DTsuper.txt - Supervisory status
  - DTtoa.txt - Types of appointment
  - DTwrksch.txt - Work schedules
  - DTwkstat.txt - Work status

*Note: March 2025 data has a different structure with multiple separate employment files and different field names. The employment cube processing pipeline automatically detects and handles this format, standardizing it to match the historical schema.

### Separations & Accessions Files
Each separations/accessions dataset contains:
- **SEPDATA_\*.TXT** or **ACCDATA_\*.TXT**: Main fact table with individual separation or accession events
- **DT\*.txt**: Lookup tables (similar to employment cube but without DTpp.txt, DTstemocc.txt, DTsuper.txt)
  - Note: Uses DTefdate.txt instead of DTdate.txt for effective date lookups

*Note: March 2025 separations/accessions data uses a different format with pipe-delimited files and pre-joined lookup tables.

## Data Sources

- **Source**: U.S. Office of Personnel Management (OPM) FedScope Employment Cube
- **Official Site**: https://www.fedscope.opm.gov/
- **License**: Public domain (U.S. Government work)

---

*This is an independent data processing project. For official federal employment statistics, visit [fedscope.opm.gov](https://www.fedscope.opm.gov/).*