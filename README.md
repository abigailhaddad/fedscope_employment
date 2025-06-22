# FedScope Employment Data (NOT OFFICIAL)

This repository contains **140+ million federal employee records** from 1998-2024, processed from the official FedScope Employment Cube datasets. 

## Quick Start

You can use this data in two ways:

### Option 1: Direct Download (Recommended)

Download individual Parquet files directly from GitHub without cloning:

```python
import pandas as pd

# Load a single quarter directly from GitHub
df = pd.read_parquet('https://github.com/abigailhaddad/fedscope_employment/raw/main/fedscope_data/parquet/fedscope_employment_September_2024.parquet')
```

Browse available files: [fedscope_data/parquet/](https://github.com/abigailhaddad/fedscope_employment/tree/main/fedscope_data/parquet)

### Option 2: Clone Repository

⚠️ **Large Repository Warning**: This repo is ~2.6GB due to the included data files. 

```bash
git clone https://github.com/abigailhaddad/fedscope_employment.git
cd fedscope_employment
```

Then load files locally:

```python
import pandas as pd
import glob

# Load one quarter
df = pd.read_parquet('fedscope_data/parquet/fedscope_employment_September_2024.parquet')

# Load all data
all_files = glob.glob('fedscope_data/parquet/*.parquet')
all_data = pd.concat([pd.read_parquet(f) for f in all_files])
```

## What's Included

- **72 quarterly snapshots** from March 1998 through September 2024
- **1.7-2.3 million employees** per quarter 
- **42 fields** including demographics, job details, and compensation
- **Lookup tables joined** for easier usage

## Example Usage

```python
# Count employees by agency
df.groupby('agysubt')['employment'].sum().sort_values(ascending=False).head(10)

# Average salary by education level
df[df['salary'].notna()].groupby('edlvlt')['salary'].mean()

# Track workforce over time
quarterly = df.groupby(['year', 'quarter'])['employment'].sum()
```

## Repository Structure

- `fedscope_data/parquet/` - 72 quarterly Parquet files (2.3GB total)
- `fedscope_data/raw/` - Original ZIP files from OPM
- `main.py` - Processing pipeline to recreate Parquet files
- [Documentation](https://abigailhaddad.github.io/fedscope_employment/) - Interactive field guide

## Data Coverage

- **1998-2008**: September only (annual snapshots)
- **2009**: September, December  
- **2010-2024**: Full quarterly coverage (March, June, September, December)

## Field Types

The dataset contains both code fields (e.g., `agelvl`) and description fields (e.g., `agelvlt`). Use the description fields ending in 't' for analysis - they contain human-readable values.

## Recreating the Dataset

The 72 quarterly ZIP files are included in `fedscope_data/raw/`. To recreate the Parquet files:

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

## Data Sources

- **Source**: U.S. Office of Personnel Management (OPM) FedScope Employment Cube
- **Official Site**: https://www.fedscope.opm.gov/
- **License**: Public domain (U.S. Government work)

---

*This is an independent data processing project. For official federal employment statistics, visit [fedscope.opm.gov](https://www.fedscope.opm.gov/).*