# FedScope Employment Data Pipeline

This repository contains the **processing pipeline** that creates a cleaned, denormalized version of the FedScope Employment Cube dataset.

**📊 Final Dataset**: https://huggingface.co/datasets/abigailhaddad/fedscope

**⚠️ Note**: This repository contains only the processing code, not the raw data files. To recreate the dataset, you'll need to download the original ZIP files from https://www.opm.gov/data/datasets/.

## About the Data

I downloaded all available FedScope Employment Cube files from the Office of Personnel Management (OPM). This dataset contains snapshots of federal civilian employment data from 1998 to 2022, totaling approximately 127 million employee records across 62 datasets.

### Data Sources and Alternatives

- **Raw Data**: The original FedScope files are available at https://www.opm.gov/data/datasets/ as individual ZIP archives that require extraction and manual merging of fact and lookup tables for each quarter
- **Official Web Interface**: OPM also provides an interactive data exploration tool at https://www.fedscope.opm.gov/ for querying the data through a web interface
- **This Processed Dataset**: For researchers and analysts who prefer working with complete, pre-processed data files, this repository provides the entire dataset as a single table

**Note**: This repository currently processes only the Employment Cube data. The FedScope system also includes Separations and Accessions data cubes, which are not included in this processed dataset.

## Data Files

The raw data consists of 62 ZIP files, one for each quarter where data is available:

### Files by Year
- **1998**: September
- **1999**: September  
- **2000**: September
- **2001**: September
- **2002**: September
- **2003**: September
- **2004**: September
- **2005**: September
- **2006**: September
- **2007**: September
- **2008**: September
- **2009**: September, December
- **2010**: March, June, September, December
- **2011**: March, June, September, December
- **2012**: March, June, September, December
- **2013**: March, June, September, December
- **2014**: March, June, September, December
- **2015**: March, June, September, December
- **2016**: March, June, September, December
- **2017**: March, June, September, December
- **2018**: March, June, September, December
- **2019**: March, June, September, December
- **2020**: March, June, September, December
- **2021**: March, June, September, December
- **2022**: March, June

## Data Preparation

The FedScope files from OPM  come with UUID-style names (e.g., `9f7eab79-e539-45fe-8e8a-00663e3ec190`) rather than descriptive names. The `fix_and_extract.py` script:

1. Identifies the time period by examining the FACTDATA filename inside each ZIP
2. Renames files to a standard format: `FedScope_Employment_{Quarter}_{Year}.zip`
3. Extracts them to matching directory names

## Repository Structure

```
fedscope_employment/
├── fedscope_data/
│   ├── raw/                    # Original ZIP files from OPM
│   └── extracted/              # Extracted data files
│       ├── FedScope_Employment_September_1998/
│       ├── FedScope_Employment_September_1999/
│       └── ... (60 more directories)
├── main.py                     # Main orchestration script
├── fix_and_extract.py          # Identifies, renames, and extracts ZIP files
├── load_to_duckdb_robust.py    # Loads data into DuckDB with schema handling
├── export_to_huggingface.py    # Exports to CSV for Hugging Face
├── validate_duckdb.py          # Validates loaded data
└── requirements.txt            # Python dependencies
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

## Pipeline Overview

The pipeline uses DuckDB as a central data warehouse that:
1. Loads all 127M+ records into a single database
2. Handles schema evolution (e.g., 'pp' column added in 2016)
3. Creates a denormalized table with all lookups joined
4. Can export to CSV for upload to Hugging Face

### Data Quality Note

Some lookup tables contain duplicate entries where agencies were renamed but kept the same code. For example, in 1998 the agency code "AMPC" appears twice:
- U.S. INTERNATIONAL DEVELOPMENT COOPERATION AGENCY
- U.S. AGENCY FOR INTERNATIONAL DEVELOPMENT

This primarily affects early years (1998-2003). When creating the denormalized table, we use the first occurrence of each duplicate and log all duplicates to `lookup_duplicates_summary.txt`. This ensures each fact record maps to exactly one description.

## Architecture

```
FedScope ZIP files → Extract → DuckDB → Export to Hugging Face
```

- **DuckDB**: Local data warehouse containing all historical data (~10GB)
- **CSV Export**: Creates a denormalized CSV file ready for Hugging Face upload

## Usage

### Full Pipeline
```bash
python main.py --all
```

### Individual Steps

1. **Extract ZIP files**:
```bash
python main.py --extract
```

2. **Load into DuckDB**:
```bash
python main.py --load-duckdb
```

3. **Export to CSV for Hugging Face**:
```bash
python main.py --export-huggingface
```

This creates `fedscope_employment_cube.csv` (~10GB) ready for upload to Hugging Face.

## Data Schema

The dataset includes for each employee record:

- **Time**: Year, quarter, dataset key
- **Demographics**: Age level, education level (with descriptions)
- **Job Characteristics**: Occupation, PATCO category, pay plan/grade, GS equivalent grade (with descriptions)
- **Compensation**: Salary, salary level (with descriptions)
- **Work Details**: Schedule, status, appointment type, supervisory status (with descriptions)
- **Organization**: Sub-agency, location (with descriptions)
- **Other**: STEM occupation indicator, length of service (with descriptions)

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Output

The pipeline produces:
- `fedscope_employment.duckdb` - Complete database with all fact and lookup tables (~10GB)
- `fedscope_employment_cube.csv` - Denormalized CSV file ready for Hugging Face upload (~10GB)
- `lookup_duplicates_summary.txt` - Documentation of data quality issues
- `lookup_duplicates_log.json` - Machine-readable duplicate records log

## License

The underlying FedScope data is in the public domain as a work of the U.S. Government.