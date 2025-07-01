# FedScope Employment Data (NOT OFFICIAL)

This repository contains **140+ million federal employee records** from 1998-2025, processed from the official FedScope Employment Cube datasets. 

**🔍 Want to see quick comparisons between September 2024 and March 2025?** See: https://fluffy-narwhal-e5f260.netlify.app/

**💻 Want to get started coding with March 2025 person-level data?** See: [September 2024 vs March 2025.ipynb](September%202024%20vs%20March%202025.ipynb)

## Quick Start

You can use this data in two ways:

### Option 1: Direct Download (Recommended)

Download individual Parquet files directly from GitHub without cloning:

```python
import pandas as pd

# Load a single quarter directly from GitHub
df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/fedscope_data/parquet/fedscope_employment_September_2024.parquet')

# For instance, load the latest March 2025 data (see warnings below)
df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/fedscope_data/parquet/fedscope_employment_March_2025.parquet')
```

Browse available files: [fedscope_data/parquet/](https://github.com/abigailhaddad/fedscope_employment/tree/main/fedscope_data/parquet)

### Option 2: Clone Repository

⚠️ **Large Repository Warning**: This repo is ~3.8GB due to the included data files. 

```bash
git clone https://github.com/abigailhaddad/fedscope_employment.git
cd fedscope_employment
```

Then load files locally:

```python
import pandas as pd

# Load one quarter (recommended - see examples.py for comprehensive usage)
df = pd.read_parquet('fedscope_data/parquet/fedscope_employment_September_2024.parquet')
```

## What's Included

- **73 quarterly snapshots** from March 1998 through March 2025
- **1.7-2.3 million employees** per quarter 
- **52 fields** including demographics, job details, and compensation
- **Lookup tables joined** for easier usage

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

> **💡 Note:** The dataset uses string types for numeric fields like `employment` and `salary`. See [examples.py](examples.py) for proper handling.

## Repository Structure

- `fedscope_data/parquet/` - 73 quarterly Parquet files (2.4GB total)
- `fedscope_data/raw/` - Original ZIP files from OPM (1.5GB total)
- `main.py` - Processing pipeline to recreate Parquet files
- `examples.py` - Comprehensive usage examples (output saved to `examples_output.txt`)
- [Additional Data Documentation](https://abigailhaddad.github.io/fedscope_employment/)

## Data Coverage

- **1998-2008**: September only (annual snapshots)
- **2009**: September, December  
- **2010-2023**: Full quarterly coverage (March, June, September, December)
- **2024**: March, June, September (December not available)
- **2025**: March only (preliminary data - see warnings above)

## Field Types

The dataset contains both code fields (e.g., `agelvl`) and description fields (e.g., `agelvlt`). Use the description fields ending in 't' for analysis - they contain human-readable values.

## Recreating the Dataset

### Historical Data (1998-2024)
The quarterly ZIP files are included in `fedscope_data/raw/`. To recreate the Parquet files:

```bash
pip install pandas pyarrow
python main.py
```

Options:
```bash
python main.py --extract     # Extract ZIP files only
python main.py --parquet     # Create Parquet files only  
python main.py --validate    # Validate Parquet files only
```

### March 2025 Data
March 2025 uses a different process due to format changes. To recreate the March 2025 Parquet file:

1. Download the 3 ZIP files from OPM and place in `fedscope_data/march_2025_data/`
2. Run the specialized processing script:

```bash
python process_march_2025.py
```

This script combines the 3 employment files and standardizes the column structure to match historical data format. Alternatively, the provided Parquet file has already been processed to ensure consistency with historical datasets.

## Repository Structure

```
fedscope_employment/
├── fedscope_data/
│   ├── raw/                    # Contains quarterly ZIP files (1998-2024)
│   ├── march_2025_data/        # March 2025 ZIP files (different format)
│   ├── extracted/              # Extracted data files (created by pipeline)
│   └── parquet/                # 73 quarterly Parquet files (final output)
├── web_dashboard/              # Interactive comparison dashboard
│   └── index.html              # Sept 2024 vs March 2025 comparison
├── main.py                     # Main orchestration script (1998-2024 data)
├── process_march_2025.py       # March 2025 processing script
├── rename_and_extract.py       # Identifies, renames, and extracts ZIP files
├── text_to_parquet.py          # Converts TXT files to Parquet with lookups
├── validate_parquet.py         # Validates Parquet files
├── examples.py                 # Usage examples and redaction analysis
└── documentation_pdfs/         # PDF documentation for each quarterly dataset
```

## Data Structure

Each quarterly dataset contains:*
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

*Note: March 2025 data has a different structure with 3 separate employment files and different field names. The `process_march_2025.py` script standardizes this to match the historical format.

## Data Sources

- **Source**: U.S. Office of Personnel Management (OPM) FedScope Employment Cube
- **Official Site**: https://www.fedscope.opm.gov/
- **License**: Public domain (U.S. Government work)

---

*This is an independent data processing project. For official federal employment statistics, visit [fedscope.opm.gov](https://www.fedscope.opm.gov/).*