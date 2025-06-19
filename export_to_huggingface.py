#!/usr/bin/env python3
"""
Export DuckDB denormalized table directly to Hugging Face Hub.
"""

import duckdb
import os
import logging
import tempfile
import pandas as pd
from huggingface_hub import HfApi, Repository
from datasets import Dataset
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"
CHUNK_SIZE = 100000  # Process 100k records at a time to manage memory

def export_to_huggingface_direct(repo_name, hf_token=None):
    """Export denormalized data directly to Hugging Face Hub."""
    if not repo_name:
        raise ValueError("repo_name is required for direct upload")
    
    logger.info("Connecting to DuckDB...")
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    try:
        # Check if denormalized table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'employment_denormalized'").fetchall()
        if not tables:
            logger.error("employment_denormalized table not found! Run the loader first.")
            return
        
        # Get total count and statistics
        total_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized").fetchone()[0]
        logger.info(f"Total records to export: {total_count:,}")
        
        year_range = conn.execute("""
            SELECT MIN(year) as min_year, MAX(year) as max_year, COUNT(DISTINCT year) as num_years
            FROM employment_denormalized
        """).fetchone()
        logger.info(f"Years covered: {year_range[0]} - {year_range[1]} ({year_range[2]} unique years)")
        
        # Initialize Hugging Face API
        api = HfApi(token=hf_token)
        
        # Create dataset repository if it doesn't exist
        try:
            api.create_repo(repo_id=repo_name, repo_type="dataset", exist_ok=True)
            logger.info(f"Repository {repo_name} ready")
        except Exception as e:
            logger.warning(f"Repository creation/verification failed: {e}")
        
        # Process data in chunks and upload directly
        logger.info(f"Processing {total_count:,} records in chunks of {CHUNK_SIZE:,}...")
        
        num_chunks = (total_count + CHUNK_SIZE - 1) // CHUNK_SIZE
        datasets_to_concatenate = []
        
        with tqdm(total=num_chunks, desc="Processing chunks") as pbar:
            for chunk_idx in range(num_chunks):
                offset = chunk_idx * CHUNK_SIZE
                
                # Get chunk data from DuckDB
                chunk_df = conn.execute(f"""
                    SELECT * FROM employment_denormalized
                    ORDER BY year, quarter, dataset_key
                    LIMIT {CHUNK_SIZE} OFFSET {offset}
                """).df()
                
                # Convert to Hugging Face Dataset
                chunk_dataset = Dataset.from_pandas(chunk_df)
                datasets_to_concatenate.append(chunk_dataset)
                
                pbar.update(1)
                logger.info(f"Processed chunk {chunk_idx + 1}/{num_chunks} ({len(chunk_df):,} records)")
        
        # Concatenate all chunks into final dataset
        logger.info("Concatenating all chunks...")
        from datasets import concatenate_datasets
        final_dataset = concatenate_datasets(datasets_to_concatenate)
        
        # Create dataset card content
        dataset_card = f"""# FedScope Employment Cube

This dataset contains the complete U.S. Office of Personnel Management (OPM) FedScope Employment Cube data from 1998-2022.

## Dataset Description

- **Total Records**: {total_count:,}
- **Time Period**: {year_range[0]} to {year_range[1]}  
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
"""
        
        # Upload dataset to Hugging Face Hub
        logger.info(f"Uploading dataset to {repo_name}...")
        final_dataset.push_to_hub(
            repo_id=repo_name,
            token=hf_token,
            commit_message="Upload FedScope Employment Cube dataset"
        )
        
        # Upload README
        logger.info("Uploading dataset card...")
        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(dataset_card)
            readme_path = f.name
        
        try:
            api.upload_file(
                path_or_fileobj=readme_path,
                path_in_repo="README.md",
                repo_id=repo_name,
                repo_type="dataset",
                token=hf_token,
                commit_message="Add dataset card"
            )
        finally:
            os.unlink(readme_path)
        
        logger.info(f"✅ Dataset successfully uploaded to https://huggingface.co/datasets/{repo_name}")
        
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise
    finally:
        conn.close()

def export_to_huggingface(push_to_hub=False, repo_name=None):
    """Legacy function - now redirects to direct upload."""
    if push_to_hub and repo_name:
        hf_token = os.getenv('HF_TOKEN') or os.getenv('HUGGINGFACE_HUB_TOKEN')
        export_to_huggingface_direct(repo_name, hf_token)
    else:
        logger.error("Direct upload requires push_to_hub=True and a valid repo_name")
        logger.info("Use export_to_huggingface_direct() for memory-efficient upload")

if __name__ == "__main__":
    export_to_huggingface(push_to_hub=True, repo_name="abigailhaddad/fedscope")