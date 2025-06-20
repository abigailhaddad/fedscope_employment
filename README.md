# FedScope Employment Data Pipeline

**📊 DATASET AVAILABLE ON HUGGING FACE**: https://huggingface.co/datasets/abigailhaddad/fedscope

This repository contains the **processing pipeline** that creates the cleaned dataset. **The actual data files (72 quarterly CSV files, ~140M records) are hosted on Hugging Face**, not in this repository.

**📚 Documentation**: https://abigailhaddad.github.io/fedscope_employment/

**⚠️ Note**: This repository contains only the processing code. The final dataset lives on Hugging Face. To recreate the dataset from scratch, you'd need to download the original ZIP files from https://www.opm.gov/data/datasets/.

## Using the Dataset

The processed dataset on Hugging Face contains 72 quarterly CSV files with ~140M federal employee records (1998-2024). Each file is ready for analysis with all lookup tables already joined.

### Quick Start

Download individual quarterly CSV files from https://huggingface.co/datasets/abigailhaddad/fedscope. 

**Note**: Each file contains 1.7-2.3 million records and may exceed spreadsheet software limits (Excel: 1,048,576 rows). Use Python, R, or other data analysis tools for full files.

**Python Example:**
```python
import pandas as pd

# Load a specific quarter directly from Hugging Face
df = pd.read_csv("https://huggingface.co/datasets/abigailhaddad/fedscope/resolve/main/fedscope_employment_September_2022.csv")

# Or download manually from https://huggingface.co/datasets/abigailhaddad/fedscope/tree/main and load locally
df = pd.read_csv("fedscope_employment_September_2022.csv")
```

### What You Get

- **Ready-to-analyze data**: No need to extract ZIPs or join lookup tables
- **Consistent schema**: Schema evolution handled (e.g., pay plan field added in 2016) 
- **Clean data**: Duplicates resolved, salary redactions converted to null
- **72 quarterly files**: Individual CSV files, 1998-2024 (1.7M-2.2M records each)

### Data Sources and Alternatives

- **This Dataset (Recommended)**: Pre-processed quarterly CSV files ready for analysis
- **Raw Data**: Original FedScope ZIP files at https://www.opm.gov/data/datasets/ (requires manual extraction and joining)
- **Official Web Interface**: https://www.fedscope.opm.gov/ (includes additional variables not in downloadable files)

**Note**: This covers only Employment Cube data. FedScope also has Separations and Accessions cubes not included here.

## Data Coverage

- **1998-2008**: September only (annual snapshots)
- **2009**: September, December  
- **2010-2024**: Full quarterly coverage (March, June, September, December)
- **2024**: Through September

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

## Data Quality

The processing pipeline handles several data quality issues:

- **Duplicate Lookup Entries**: Early years (1998-2003) contain duplicate agency entries with same codes but different names. The pipeline uses the first occurrence and logs all duplicates.
- **Schema Evolution**: The pay plan field was added in 2016, so earlier years have null values.
- **Redacted Values**: Some values are masked with asterisks (*, **, ***, ****) in the source data and converted to null for clean analysis.

## Recreating This Dataset

If you want to recreate the dataset from scratch:

### Prerequisites
1. Download all 72 quarterly **FedScope Employment Cube** ZIP files from https://www.opm.gov/data/datasets/
2. Place them in `fedscope_data/raw/` directory
3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Pipeline Steps
```bash
# Full pipeline
python main.py --all

# Or individual steps:
python main.py --extract           # Extract ZIP files
python main.py --load-duckdb       # Load into DuckDB
python export_and_upload_one_by_one.py <repo-name>  # Upload to HF
```

### Architecture
```
FedScope ZIP files → Extract → DuckDB → Upload quarterly CSV files to Hugging Face
```

The pipeline uses DuckDB as a local data warehouse (~10GB) to:
1. Load all 140M+ records with schema handling
2. Create denormalized tables with lookups joined
3. Export and upload quarterly CSV files to Hugging Face

### Output Files
- `fedscope_employment.duckdb` - Complete local database
- `lookup_duplicates_summary.txt` - Data quality documentation  
- `documentation_pdfs/` - Original OPM documentation PDFs

## License

The underlying FedScope data is in the public domain as a work of the U.S. Government.