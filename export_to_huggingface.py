#!/usr/bin/env python3
"""
Export DuckDB denormalized table to CSV for Hugging Face upload.
"""

import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"
OUTPUT_FILE = "fedscope_employment_cube.csv"

def export_to_huggingface(push_to_hub=False, repo_name=None):
    """Export denormalized data to CSV for Hugging Face."""
    logger.info("Connecting to DuckDB...")
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    try:
        # Check if denormalized table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'employment_denormalized'").fetchall()
        if not tables:
            logger.error("employment_denormalized table not found! Run the loader first.")
            return
        
        # Get total count
        total_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized").fetchone()[0]
        logger.info(f"Total records to export: {total_count:,}")
        
        # Export to CSV
        logger.info(f"Exporting to {OUTPUT_FILE}...")
        conn.execute(f"""
            COPY (
                SELECT * FROM employment_denormalized
                ORDER BY year, quarter, dataset_key
            ) TO '{OUTPUT_FILE}' (HEADER, DELIMITER ',')
        """)
        
        # Get file size
        file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024 * 1024)  # GB
        logger.info(f"Export complete! File size: {file_size:.2f} GB")
        
        # Get some statistics
        logger.info("\nDataset statistics:")
        
        year_range = conn.execute("""
            SELECT MIN(year) as min_year, MAX(year) as max_year, COUNT(DISTINCT year) as num_years
            FROM employment_denormalized
        """).fetchone()
        logger.info(f"Years covered: {year_range[0]} - {year_range[1]} ({year_range[2]} unique years)")
        
        quarter_counts = conn.execute("""
            SELECT quarter, COUNT(*) as cnt 
            FROM employment_denormalized 
            GROUP BY quarter 
            ORDER BY quarter
        """).fetchall()
        logger.info("Records by quarter:")
        for quarter, count in quarter_counts:
            logger.info(f"  {quarter}: {count:,}")
        
        # Dataset metadata for Hugging Face
        logger.info("\n" + "="*50)
        logger.info("Dataset Card Information:")
        logger.info("="*50)
        logger.info("""
# FedScope Employment Cube

This dataset contains the complete U.S. Office of Personnel Management (OPM) FedScope Employment Cube data from 1998-2022.

## Dataset Description

- **Total Records**: {:,}
- **Time Period**: {} to {}  
- **Update Frequency**: Quarterly snapshots
- **Source**: https://www.opm.gov/data/datasets/

## What's Included

Each row represents an anonymized federal employee record with:
- Demographics (age level, education level)
- Job characteristics (occupation, grade, pay plan)
- Compensation (salary, salary level)
- Work details (schedule, appointment type, supervisory status)
- Organization (agency, sub-agency, location)
- STEM occupation indicator
- Length of service

All coded values include both the code and human-readable description.

## Data Quality Notes

Some lookup tables contained duplicate entries in early years (1998-2003) where agencies were renamed but kept the same code. 
We used the first occurrence of each duplicate. See `lookup_duplicates_summary.txt` in the source repository for details.

## Source Repository

https://github.com/abigailhaddad/fedscope_employment

## License

The underlying FedScope data is in the public domain as a work of the U.S. Government.
        """.format(total_count, year_range[0], year_range[1]))
        
        if push_to_hub:
            logger.info(f"\nTo upload to Hugging Face Hub:")
            logger.info(f"1. Install git-lfs: git lfs install")
            logger.info(f"2. Clone your dataset repo: git clone https://huggingface.co/datasets/{repo_name}")
            logger.info(f"3. Copy {OUTPUT_FILE} to the cloned directory")
            logger.info(f"4. Create README.md with the dataset card above")
            logger.info(f"5. Git add, commit, and push")
            logger.info(f"\nOr use the Hugging Face web interface to upload the CSV directly.")
            
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    export_to_huggingface(push_to_hub=True, repo_name="abigailhaddad/fedscope")