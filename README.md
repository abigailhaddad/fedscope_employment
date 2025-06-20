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

# Federal Employment Data (1998-2024)

Ready-to-analyze federal civilian employment data: **72 quarterly snapshots** with **140+ million employee records** from 1998 to 2024.

**⚠️ This is unofficial processed data.** Official data is at [fedscope.opm.gov](https://www.fedscope.opm.gov/).

## Quick Start

Download CSV files from [this Hugging Face dataset](https://huggingface.co/datasets/abigailhaddad/fedscope/tree/main) and load with Python, R, or your preferred tool.

**Note**: Files are large (1.7-2.3M rows each) and exceed Excel's limits. The Hugging Face dataset preview may show errors, but data loads correctly.

```python
import pandas as pd

# Load September 2024 data
df = pd.read_csv("https://huggingface.co/datasets/abigailhaddad/fedscope/resolve/main/fedscope_employment_September_2024.csv")

# Or download manually and load locally
df = pd.read_csv("fedscope_employment_September_2024.csv")
```

## What You Can Analyze

Each row is an anonymized federal employee with their:
- **Demographics**: Age group, education level
- **Job**: Occupation, grade level, pay plan, supervisory status  
- **Organization**: Agency, sub-agency, location
- **Compensation**: Salary level (some redacted for privacy)
- **Work details**: Schedule (full/part-time), appointment type

Useful for analyzing federal workforce trends, compensation patterns, geographic distribution, and demographic changes over time.

## Key Fields for Analysis

| Field | Description | Example Values |
|-------|-------------|----------------|
| `year`, `quarter` | When the data was collected | 2024, "September" |
| `agy`, `agysubt` | Agency and sub-agency names | "Department of Defense", "Army" |
| `loct` | Work location | "DISTRICT OF COLUMBIA", "CALIFORNIA" |
| `occt` | Job title/occupation | "Accountant", "Computer Scientist" |
| `patcot` | Job category | "Professional", "Administrative" |
| `edlvlt` | Education level | "Bachelor's Degree", "Master's Degree" |
| `agelvlt` | Age group | "25-29 YEARS", "45-49 YEARS" |
| `salary` | Annual salary | 65000, null (when redacted) |
| `sallvlt` | Salary range | "$60,000 - $69,999" |
| `employment` | Employee count | 1 (each row = 1 person) |

**[→ See all 42 fields](https://abigailhaddad.github.io/fedscope_employment/)** with detailed descriptions and coding schemes.

## Data Coverage

- **1998-2008**: September only (annual snapshots)
- **2009**: September, December  
- **2010-2024**: Quarterly (March, June, September, December)
- **2024**: Through September

Each file contains all federal civilian employees for that quarter (~1.7-2.3 million people).

## Important Notes

**Field Types**: Each concept has two fields - a code (for joining/filtering) and description (for analysis). Use the description fields ending in 't' for most analysis (e.g., `occt` not `occ`, `loct` not `loc`).

**Missing Data**: Some values are null when:
- Data was redacted for privacy (especially salaries)
- Field didn't exist in earlier years (pay plan added 2016)
- Information was legitimately missing

**Agency Changes**: Government reorganizations over 26 years mean agency names and codes change. Early years (1998-2003) have some duplicate agency entries.

## Example Uses

- Track federal workforce size changes over time
- Analyze compensation patterns by occupation or agency
- Map federal employment by state/region
- Study workforce demographics and tenure
- Compare agencies by size and composition
- Examine seasonal employment patterns

## Links

- **📊 This Dataset**: [Hugging Face repository](https://huggingface.co/datasets/abigailhaddad/fedscope)
- **📚 Full Documentation**: [Field guide and background](https://abigailhaddad.github.io/fedscope_employment/)
- **🔧 Processing Pipeline**: [Development README](https://github.com/abigailhaddad/fedscope_employment/blob/main/README-DEVELOPMENT.md)
- **🏛️ Official Source**: [OPM FedScope](https://www.fedscope.opm.gov/)

## Why Use This vs. Official Data?

The [official FedScope data](https://www.opm.gov/data/datasets/) requires downloading, extracting, and joining 13+ lookup tables for each quarter you want to analyze. For the full historical dataset, that's downloading 72 ZIP files, extracting them, and performing nearly 1,000 table joins. This processed version gives you analysis-ready CSV files with all the joins already done.

## Citation

```
Haddad, A. (2024). FedScope Employment Cube Dataset [Data set]. 
Hugging Face. https://huggingface.co/datasets/abigailhaddad/fedscope
```

## License

Public domain (CC0) - U.S. Government data.