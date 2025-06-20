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

### Time Dimensions
- `year`: Calendar year (1998-2024)
- `quarter`: Quarter (March, June, September, December)
- `dataset_key`: Unique identifier for each quarterly dataset

### Demographics
- `agelvl`: Age level (with description)
- `edlvl`: Education level (with description)

### Job Characteristics  
- `occ`: Occupation code and series (with description)
- `patco`: Professional/Administrative/Technical/Clerical/Other category (with description)
- `pp`: Pay plan (with description)
- `ppgrd`: Pay plan and grade (with description)
- `gsegrd`: General Schedule equivalent grade (with description)

### Compensation
- `salary`: Annual salary (null values indicate redacted/masked salaries)
- `sallvl`: Salary level range (with description)

### Work Details
- `wrksch`: Work schedule (full-time, part-time, etc.) (with description)
- `wkstat`: Work status (with description)
- `toa`: Type of appointment (with description)
- `super`: Supervisory status (with description)

### Organization
- `agy`: Agency code (with description)
- `subagy`: Sub-agency code (with description)  
- `loc`: Location code (with description)

### Additional Information
- `stemocc`: STEM occupation indicator (with description)
- `los`: Length of service (with description)

All fields include both codes and descriptions.

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
- **Redacted Salaries:** Some salary values are masked in source data (replaced with null)
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