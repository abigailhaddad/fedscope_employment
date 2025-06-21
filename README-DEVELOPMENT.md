# FedScope Employment Data Pipeline

This repository contains the **processing pipeline** that creates the cleaned dataset. **The actual data files (72 quarterly CSV files, ~140M records) are hosted on [Hugging Face](https://huggingface.co/datasets/abigailhaddad/fedscope)**.

## What's in This Repository

- **Original ZIP files**: All 72 quarterly FedScope Employment Cube ZIP files are included in the `fedscope_data/raw/` directory for easy replication
- **Processing pipeline**: Scripts to extract, clean, and transform the data
- **Documentation**: Original OPM documentation PDFs for each quarterly dataset

If you want the raw ZIP files in one location rather than downloading from multiple OPM pages, or if you want to replicate the processing, you can use this repository.

## Data Quality

The processing pipeline handles several data quality issues:

- **Duplicate Lookup Entries**: Early years (1998-2003) contain duplicate agency entries with same codes but different names. The pipeline uses the first occurrence and logs all duplicates.
- **Schema Evolution**: The pay plan field was added in 2016, so earlier years have null values.
- **Redacted Values**: Some values are masked with asterisks (*, **, ***, ****) in the source data and converted to null for clean analysis.

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

## Recreating This Dataset

If you want to recreate the dataset from scratch:

### Prerequisites
1. The 72 quarterly ZIP files are already included in this repository in the `fedscope_data/raw/` directory. No need to download them separately!
   
   *(Alternative: If you prefer to download fresh copies, get all 72 quarterly **FedScope Employment Cube** ZIP files from https://www.opm.gov/data/datasets/ and place them in `fedscope_data/raw/`)*

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Pipeline Steps
```bash
# Default: Extract and load into DuckDB
python main.py

# Or individual steps:
python main.py --extract           # Extract ZIP files only
python main.py --load-duckdb       # Load into DuckDB only
python main.py --all              # Same as no arguments

# To upload to Hugging Face (optional):
python export_and_upload_one_by_one.py <repo-name>
```

### What the Pipeline Does

Running `python main.py` will:

1. **Extract** all 72 ZIP files from `fedscope_data/raw/`
2. **Parse** the fact data files (FACTDATA_*.TXT) and lookup tables (DT*.txt) from each quarter
3. **Merge** the lookup tables with the fact data, replacing codes with human-readable descriptions
4. **Concatenate** all 72 quarters of data into a single database
5. **Produce** a DuckDB database file (`fedscope_employment.duckdb`) containing:
   - All 140+ million employee records from 1998-2024
   - Fully denormalized data with all lookups joined
   - Ready for analysis with SQL queries

The resulting DuckDB file (~10GB) serves as a local data warehouse that can be queried directly or exported to other formats.

### Architecture
```
72 ZIP files → Extract → Parse fact & lookup tables → Join lookups → Load into DuckDB → Single queryable database
```

### Output Files
- `fedscope_employment.duckdb` - Complete local database with all 140M+ records
- `lookup_duplicates_summary.txt` - Data quality documentation  
- `documentation_pdfs/` - Original OPM documentation PDFs

### Using the DuckDB Database

Once created, you can query the database directly:

```python
import duckdb

# Connect to the database
conn = duckdb.connect('fedscope_employment.duckdb')

# Example query: Count employees by year
result = conn.execute("""
    SELECT year, COUNT(*) as employee_count 
    FROM fact_data_denormalized 
    GROUP BY year 
    ORDER BY year
""").fetchall()

# Or export specific quarters to CSV
conn.execute("""
    COPY (SELECT * FROM fact_data_denormalized WHERE year = 2024 AND quarter = 'September') 
    TO 'fedscope_2024_september.csv' (HEADER, DELIMITER ',')
""")
```

## Repository Structure

```
fedscope_employment/
├── fedscope_data/
│   ├── raw/                    # Contains all 72 quarterly ZIP files
│   └── extracted/              # Extracted data files (created by pipeline)
├── main.py                     # Main orchestration script
├── fix_and_extract.py          # Identifies, renames, and extracts ZIP files
├── load_to_duckdb_robust.py    # Loads data into DuckDB with schema handling
├── export_and_upload_one_by_one.py  # Exports and uploads quarterly CSV files
├── validate_duckdb.py          # Validates loaded data
├── upload_readme.py            # Uploads README to Hugging Face
├── run_upload.sh              # Runs upload with caffeinate
├── requirements.txt           # Python dependencies
└── documentation_pdfs/        # PDF documentation for each quarterly dataset
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

The FedScope files from OPM come with UUID-style names (e.g., `9f7eab79-e539-45fe-8e8a-00663e3ec190`) rather than descriptive names. The `fix_and_extract.py` script:

1. Identifies the time period by examining the FACTDATA filename inside each ZIP
2. Renames files to a standard format: `FedScope_Employment_{Quarter}_{Year}.zip`
3. Extracts them to matching directory names

## License

The underlying FedScope data is in the public domain as a work of the U.S. Government.