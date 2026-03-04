# [DEPRECATED] FedScope Employment Data

> **This repository is deprecated and no longer maintained.** OPM has launched [Federal Workforce Data (FWD)](https://data.opm.gov/) as the replacement for FedScope, with monthly data updates, interactive visualizations, and downloadable datasets.
>
> **For up-to-date federal workforce data, use:**
> - **[data.opm.gov](https://data.opm.gov/)** — OPM's official Federal Workforce Data site (replaces fedscope.opm.gov)
> - **[fedscope_new](https://github.com/abigailhaddad/fedscope_new)** — Actively maintained repo with data from data.opm.gov (January 2021–November 2025), queryable via DuckDB/HuggingFace with no download required

---

## What was this repo?

This repository contained **140+ million federal employee records** from 1998–2025 and **9.8+ million separations/accessions records** from 2005–2025, processed from the legacy FedScope datasets at fedscope.opm.gov. It is no longer being updated.

## Why deprecated?

OPM replaced FedScope with [Federal Workforce Data (FWD)](https://data.opm.gov/) in January 2026, offering:

- **Monthly data updates** (vs. quarterly under FedScope)
- **Interactive visualizations** across workforce size, changes, location, compensation, recruitment, and demographics
- **Downloadable datasets** in modern formats
- **Improved transparency** around data quality

The [fedscope_new](https://github.com/abigailhaddad/fedscope_new) repo provides an easy way to work with this data programmatically, including parquet files hosted on HuggingFace that can be queried directly without downloading.

## Migrating to fedscope_new

The [fedscope_new](https://github.com/abigailhaddad/fedscope_new) repo covers three datasets from January 2021 through November 2025:

- **Employment** (workforce snapshots)
- **Accessions** (new hires)
- **Separations** (departures)

Query data directly via DuckDB without any local setup:

```python
import duckdb

url = "https://huggingface.co/datasets/abigailhaddad/opm-federal-employment-202511/resolve/main/data/train-00000-of-00001.parquet"
df = duckdb.execute(f"SELECT * FROM read_parquet('{url}')").df()
```

Or use the [interactive Colab notebook](https://github.com/abigailhaddad/fedscope_new) — no auth or local setup required.

## Archived content

The historical data and processing code in this repository remain available as-is for reference. The data files cover:

- **Employment Cube**: 73 quarterly snapshots (March 1998–March 2025) in `employment_cube/parquet/`
- **Separations & Accessions**: 10 multi-year files (FY2005–2025) in `separations_accessions/parquet/`
- **Processing code**: `code/` directory with extraction and denormalization pipelines
- **Analyses**: Quarto reports and Jupyter notebooks in `quarto_analyses/` and `analysis/`

For details on the archived data structure, see the [git history of this README](https://github.com/abigailhaddad/fedscope_employment/blob/main/README.md).

## Data Sources

- **Current**: [data.opm.gov](https://data.opm.gov/) (Federal Workforce Data)
- **Legacy**: [fedscope.opm.gov](https://www.fedscope.opm.gov/) (now redirects to FWD)
- **License**: Public domain (U.S. Government work)

---

*This was an independent data processing project. For official federal employment statistics, visit [data.opm.gov](https://data.opm.gov/).*
