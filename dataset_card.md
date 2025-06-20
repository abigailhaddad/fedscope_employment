---
license: cc0-1.0
task_categories:
- tabular-classification
- tabular-regression
language:
- en
tags:
- government
- employment
- federal
- economics
- demographics
- time-series
- public-sector
- hr
- workforce-analysis
size_categories:
- 100M<n<1B
---

# FedScope Employment Cube Dataset

Cleaned and processed version of OPM's FedScope Employment Cube data (1998-2024), containing ~140M federal employee records across 72 quarterly snapshots.

**⚠️ UNOFFICIAL processed copy. NOT an official government dataset.**

## Quick Usage

**Note**: Each file contains 1.7-2.3 million records, exceeding Excel's limit (1,048,576 rows). Use Python/R for analysis.

```python
import pandas as pd

# Load a specific quarter directly
df = pd.read_csv("https://huggingface.co/datasets/abigailhaddad/fedscope/resolve/main/fedscope_employment_September_2022.csv")
```

## Dataset Details

Each record represents an anonymized federal employee with demographic, job, and organizational information. 

- **Source:** OPM's FedScope Employment Cube files (https://www.opm.gov/data/datasets/)
- **Processing:** Denormalized fact/lookup tables into analysis-ready CSVs
- **Note:** OPM's web interface (https://www.fedscope.opm.gov/) includes additional variables not in downloadable files
- **License:** CC0 1.0 (Public Domain)

### Links

- **Code & Docs:** https://github.com/abigailhaddad/fedscope_employment
- **Field Guide:** https://abigailhaddad.github.io/fedscope_employment/
- **Original Data:** https://www.opm.gov/data/datasets/

## Fields

### Field Format Structure

The dataset contains three types of fields:

1. **Code Fields**: Original FedScope codes used for categorization and joining (e.g., `agelvl`, `edlvl`, `occ`)
2. **Description Fields**: Human-readable labels derived from lookup tables (e.g., `agelvlt`, `edlvlt`, `occt`)
3. **Data Fields**: Actual analytical values (e.g., `employment`, `salary`, `year`)

**Naming Pattern**: Description fields follow the pattern of adding 't' to the code field name (e.g., `agelvl` → `agelvlt`).

### Time Dimensions
- `dataset_key`: Unique identifier for each quarterly dataset (string)
- `year`: Calendar year (1998-2024)
- `quarter`: Quarter name (March, June, September, December)

### Demographics (Codes + Descriptions)
- `agelvl` / `agelvlt`: Age level code / Age level description (5-year bands)
- `edlvl` / `edlvlt`: Education level code / Education level description

### Job Characteristics (Codes + Descriptions)
- `occ` / `occt`: Occupation code / Occupation title
- `occfam` / `occfamt`: Occupation family code / Occupation family description
- `patco` / `patcot`: PATCO category code / PATCO category description
- `pp` / `ppt`: Pay plan code / Pay plan description (null before 2016)
- `ppgrd` / `ppgrdt`: Pay plan and grade code / Pay plan and grade description
- `gsegrd` / `gsegrdt`: GS equivalent grade code / GS equivalent grade description
- `supervis` / `supervist`: Supervisory status code / Supervisory status description

### Compensation (Codes + Descriptions + Data)
- `salary`: Annual basic pay amount (null when redacted with asterisks)
- `sallvl` / `sallvlt`: Salary level code / Salary level description

### Work Details (Codes + Descriptions)
- `wrksch` / `wrkscht`: Work schedule code / Work schedule description
- `wkstat` / `wkstatt`: Work status code / Work status description
- `toa` / `toat`: Type of appointment code / Type of appointment description
- `loslvl` / `loslvlt`: Length of service level code / Length of service level description

### Organization (Codes + Descriptions)
- `agysub` / `agysubt`: Sub-agency code / Sub-agency description
- `agy`: Agency code (from lookup join)
- `loc` / `loct`: Location code / Location description

### Additional Information (Codes + Descriptions + Data)
- `stemocc` / `stemocct`: STEM occupation indicator code / STEM occupation description
- `los`: Length of service (years)
- `datecode`: Date code
- `employment`: Employment count

**Note**: Code fields are primarily used for data processing and joins. For analysis, use the description fields (ending in 't') which provide human-readable values.

## Dataset Creation

### Curation Rationale

The original FedScope data is published as separate quarterly ZIP files with fact and lookup tables that need to be extracted and merged. Working with multiple datasets requires downloading, unzipping, and joining tables repeatedly. This processed dataset addresses several practical challenges:

1. **Accessibility**: Processes 72 separate quarterly datasets into individual CSV files ready for analysis
2. **Usability**: Joins all lookup tables to provide human-readable descriptions
3. **Consistency**: Handles schema evolution (e.g., pay plan field added in 2016)
4. **Quality**: Documents and resolves duplicate entries in lookup tables

### Source Data

#### Data Collection and Processing

Processing pipeline:
1. Downloads and extracts 72 quarterly ZIP files
2. Identifies time periods from FACTDATA filenames  
3. Handles schema evolution across 26+ years
4. Joins fact tables with 13 lookup tables
5. Resolves duplicate lookup entries
6. Exports as individual CSV files

### Data Availability
- **1998-2008:** September only
- **2009:** September, December
- **2010-2024:** March, June, September, December (2024 through September)

### Privacy

No personally identifiable information. All records are anonymous with only aggregate categories.

## Bias, Risks, and Limitations

- **Lookup Duplicates:** 1998-2003 contain duplicate agency entries (same code, different names)
- **Schema Changes:** Pay plan field added in 2016 (null in earlier years)
- **Redacted Values:** Some values are masked with asterisks in source data (replaced with null)
- **Snapshots:** Quarterly point-in-time data, not continuous employment histories
- **Aggregated:** Demographics/geography grouped into ranges


## Citation

**BibTeX:**
```bibtex
@dataset{haddad2024fedscope,
  title={FedScope Employment Cube Dataset},
  author={Haddad, Abigail},
  year={2024},
  publisher={Hugging Face},
  url={https://huggingface.co/datasets/abigailhaddad/fedscope},
  note={Processed from U.S. Office of Personnel Management FedScope data}
}
```

**APA:**
Haddad, A. (2024). FedScope Employment Cube Dataset [Data set]. Hugging Face. https://huggingface.co/datasets/abigailhaddad/fedscope

## Glossary

- **FedScope**: Federal employment data system maintained by OPM
- **EHRI**: Enterprise Human Resources Integration - OPM's federal HR data system
- **GS**: General Schedule - primary federal pay system
- **PATCO**: Professional, Administrative, Technical, Clerical, Other - job classification system
- **OPM**: Office of Personnel Management - federal HR agency
- **STEM**: Science, Technology, Engineering, Mathematics occupations

## Contact

- **This dataset:** https://github.com/abigailhaddad/fedscope_employment/issues
- **Official data:** https://www.opm.gov/data/

Processed by Abigail Haddad