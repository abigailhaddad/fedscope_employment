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

This dataset is a cleaned and processed version of the official U.S. Office of Personnel Management (OPM) FedScope Employment Cube data from 1998-2024, comprising approximately 140+ million employee records across 72 quarterly datasets.

**⚠️ DISCLAIMER: This is an UNOFFICIAL processed copy of federal data. This is NOT an official government dataset.**

## Dataset Details

### Dataset Description

This is a cleaned and processed version of the official FedScope Employment Cube data collected quarterly by the U.S. Office of Personnel Management. Each record represents an anonymized federal employee with demographic, occupational, compensation, and organizational information. 

**Data Source**: The original data comes from OPM's official FedScope Employment Cube files available at https://www.opm.gov/data/datasets/. This processed dataset has been denormalized from the original format to provide a single, analysis-ready table.

**Note**: OPM also provides a Cognos-based web interface at https://www.fedscope.opm.gov/ that includes additional variables not available in the downloadable cube files. This dataset is based on the downloadable cube data only.

- **Original Data Curated by:** U.S. Office of Personnel Management (OPM)
- **Original Data Source:** https://www.opm.gov/data/datasets/
- **Data Processing by:** Abigail Haddad
- **Funded by:** U.S. Government (original data collection)
- **Language(s):** English (field names and categorical descriptions)
- **License:** CC0 1.0 (Public Domain)

### Dataset Sources

- **Processing Code Repository:** https://github.com/abigailhaddad/fedscope_employment
- **Original Authoritative Data Source:** https://www.opm.gov/data/datasets/
- **Official OPM Web Interface:** https://www.fedscope.opm.gov/ (includes additional variables not in downloadable files)
- **Original Data Format:** 72 separate quarterly ZIP files with fact and lookup tables

## Uses

### Direct Use

This dataset can be used to analyze federal government employment patterns over time, including trends in demographics, compensation, job classifications, and agency staffing from 1998-2024.

### Out-of-Scope Use

- **Official Government Use**: This is NOT an official dataset and should not be used for official government decisions or reporting
- **Individual Identification**: Data has been anonymized and cannot be used to identify specific employees

## Dataset Structure

The dataset contains the following key fields for each employee record:

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
- `salary`: Annual salary
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

All coded fields include both the original code and human-readable description for ease of analysis.

## Dataset Creation

### Curation Rationale

The original FedScope data is published as separate quarterly ZIP files with complex fact and lookup table structures that require significant technical expertise to merge and analyze. This processed dataset addresses several key challenges:

1. **Accessibility**: Combines 72 separate quarterly datasets into a single table
2. **Usability**: Joins all lookup tables to provide human-readable descriptions
3. **Consistency**: Handles schema evolution (e.g., pay plan field added in 2016)
4. **Quality**: Documents and resolves duplicate entries in lookup tables

### Source Data

#### Data Collection and Processing

**Original Data Collection**: The source data comes from the U.S. Office of Personnel Management's Enterprise Human Resources Integration (EHRI) system, which maintains records for all federal civilian employees. OPM publishes this data quarterly as individual ZIP files at https://www.opm.gov/data/datasets/.

**This Dataset's Processing Pipeline**:

1. **Extraction**: Downloads and extracts 72 quarterly ZIP files from OPM
2. **Identification**: Identifies time periods from FACTDATA filenames  
3. **Schema Handling**: Manages evolving schema across 26+ years
4. **Lookup Resolution**: Joins fact tables with 13 different lookup tables
5. **Quality Control**: Handles duplicate lookup entries (primarily 1998-2003)
6. **Denormalization**: Creates single analysis-ready table

#### Who are the source data producers?

The data is produced by the U.S. Office of Personnel Management (OPM) as part of their statutory responsibility to maintain federal workforce statistics. OPM collects this data from federal agencies' human resources systems on a quarterly basis.

### Personal and Sensitive Information

The dataset contains no personally identifiable information. All employee records are anonymous and contain only aggregate demographic categories, job classifications, and salary ranges. The original data collection follows federal privacy guidelines and OPM disclosure policies.

## Bias, Risks, and Limitations

### Data Quality Issues
- **Lookup Duplicates**: Some early years (1998-2003) contain duplicate agency entries where agencies were renamed but retained the same code
- **Schema Changes**: The pay plan field was added in 2016, resulting in null values for earlier years
- **Reporting Variations**: Different agencies may have varying data quality and reporting practices


### Technical Limitations
- **Quarterly Snapshots**: Data represents point-in-time snapshots, not continuous employment histories
- **Aggregated Categories**: Some demographic and geographic information is grouped into ranges rather than precise values


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

## More Information

For technical details about data processing, schema handling, and quality issues, see the source repository: https://github.com/abigailhaddad/fedscope_employment

For questions about the original data collection methodology, contact the U.S. Office of Personnel Management at https://www.opm.gov/data/

## Dataset Card Authors

Abigail Haddad

## Dataset Card Contact

For questions about this processed dataset, please open an issue at: https://github.com/abigailhaddad/fedscope_employment/issues

For official data inquiries, contact the U.S. Office of Personnel Management directly.