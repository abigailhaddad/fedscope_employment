#!/usr/bin/env python3
"""
Export individual CSV files and upload them one by one to Hugging Face, then delete.
Validates uploaded data matches DuckDB source.
"""

import duckdb
import os
import logging
from dotenv import load_dotenv
from huggingface_hub import HfApi
from datasets import Dataset, load_dataset
import pandas as pd

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"
TEMP_CSV = "temp_fedscope_quarter.csv"

def validate_uploaded_data(conn, repo_name, datasets, hf_token):
    """Validate that uploaded data matches DuckDB source."""
    logger.info("Validating uploaded data against DuckDB source...")
    
    try:
        # Get DuckDB totals by quarter
        duckdb_totals = {}
        for dataset_key, year, quarter in datasets:
            count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = ?", [dataset_key]).fetchone()[0]
            config_name = f"{quarter}_{year}"
            duckdb_totals[config_name] = count
        
        logger.info(f"DuckDB has {len(duckdb_totals)} quarterly datasets")
        
        # Check what's actually uploaded to HF
        api = HfApi(token=hf_token)
        uploaded_files = api.list_repo_files(repo_id=repo_name, repo_type="dataset")
        data_files = [f for f in uploaded_files if f.endswith('.parquet') or f.endswith('.csv')]
        
        logger.info(f"Hugging Face has {len(data_files)} data files")
        
        # Try to load each dataset config and count records
        hf_totals = {}
        validation_errors = []
        
        for config_name, expected_count in duckdb_totals.items():
            try:
                # Load this specific config
                dataset = load_dataset(repo_name, name=config_name, token=hf_token)
                actual_count = len(dataset['train'])
                hf_totals[config_name] = actual_count
                
                if actual_count == expected_count:
                    logger.info(f"✅ {config_name}: {actual_count:,} records (matches DuckDB)")
                else:
                    error_msg = f"❌ {config_name}: {actual_count:,} records (expected {expected_count:,})"
                    logger.error(error_msg)
                    validation_errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"❌ {config_name}: Could not load from HF - {e}"
                logger.error(error_msg)
                validation_errors.append(error_msg)
        
        # Summary
        logger.info(f"\n=== VALIDATION SUMMARY ===")
        logger.info(f"DuckDB total records: {sum(duckdb_totals.values()):,}")
        logger.info(f"Hugging Face total records: {sum(hf_totals.values()):,}")
        logger.info(f"Datasets validated: {len(hf_totals)}/{len(duckdb_totals)}")
        
        if validation_errors:
            logger.error(f"Validation failed! {len(validation_errors)} errors:")
            for error in validation_errors:
                logger.error(f"  {error}")
        else:
            logger.info("🎉 All validation checks passed!")
            
        # Print dataset info
        logger.info(f"\n=== DATASET INFO ===")
        logger.info(f"Repository: https://huggingface.co/datasets/{repo_name}")
        logger.info(f"Time range: {min(year for _, year, _ in datasets)} - {max(year for _, year, _ in datasets)}")
        logger.info(f"Quarterly files: {len(datasets)}")
        logger.info(f"Total records: {sum(duckdb_totals.values()):,}")
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}")

def export_and_upload_one_by_one(repo_name, hf_token=None):
    """Export CSV files one by one, upload each, then delete."""
    if not repo_name:
        raise ValueError("repo_name is required")
    
    logger.info("Connecting to DuckDB...")
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    try:
        # Check if denormalized table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'employment_denormalized'").fetchall()
        if not tables:
            logger.error("employment_denormalized table not found! Run the loader first.")
            return
        
        # Initialize Hugging Face API
        api = HfApi(token=hf_token)
        
        # Create dataset repository if it doesn't exist
        try:
            api.create_repo(repo_id=repo_name, repo_type="dataset", exist_ok=True)
            logger.info(f"Repository {repo_name} ready")
        except Exception as e:
            logger.warning(f"Repository creation/verification failed: {e}")
        
        # Clear existing files in repository (except .gitattributes)
        logger.info("Clearing existing files from repository...")
        try:
            existing_files = api.list_repo_files(repo_id=repo_name, repo_type="dataset")
            files_to_delete = [f for f in existing_files if f != '.gitattributes']
            
            if files_to_delete:
                logger.info(f"Deleting {len(files_to_delete)} existing files: {files_to_delete}")
                for file_path in files_to_delete:
                    api.delete_file(
                        path_in_repo=file_path,
                        repo_id=repo_name,
                        repo_type="dataset",
                        token=hf_token,
                        commit_message=f"Delete {file_path} before fresh upload"
                    )
                logger.info("✅ Repository cleared")
            else:
                logger.info("Repository already clean")
        except Exception as e:
            logger.warning(f"Could not clear repository: {e}")
        
        # Get total count and year range
        total_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized").fetchone()[0]
        year_range = conn.execute("SELECT MIN(year), MAX(year) FROM employment_denormalized").fetchone()
        logger.info(f"Total records: {total_count:,}")
        logger.info(f"Years: {year_range[0]} - {year_range[1]}")
        
        # Get list of dataset keys (each represents a quarter/year file)
        datasets = conn.execute("SELECT DISTINCT dataset_key, year, quarter FROM employment_denormalized ORDER BY year, quarter").fetchall()
        
        logger.info(f"Processing {len(datasets)} quarterly datasets one by one...")
        
        total_processed = 0
        for i, (dataset_key, year, quarter) in enumerate(datasets, 1):
            csv_filename = f"fedscope_employment_{quarter}_{year}.csv"
            
            # Get count for this dataset
            dataset_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = ?", [dataset_key]).fetchone()[0]
            
            logger.info(f"[{i}/{len(datasets)}] Processing {quarter} {year}: {dataset_count:,} records...")
            
            # Export this dataset to temporary CSV
            logger.info(f"  Exporting to {TEMP_CSV}...")
            conn.execute(f"""
                COPY (
                    SELECT * FROM employment_denormalized
                    WHERE dataset_key = '{dataset_key}'
                    ORDER BY dataset_key
                ) TO '{TEMP_CSV}' (HEADER, DELIMITER ',')
            """)
            
            # Get file size
            file_size = os.path.getsize(TEMP_CSV) / (1024 * 1024)  # MB
            logger.info(f"  Created {TEMP_CSV} ({file_size:.1f} MB)")
            
            # Convert to Hugging Face Dataset and upload
            logger.info(f"  Converting to HF Dataset and uploading as {csv_filename}...")
            # Read everything as strings to avoid type conversion issues
            df = pd.read_csv(TEMP_CSV, dtype=str, na_filter=False)
            dataset = Dataset.from_pandas(df)
            
            # Upload to Hugging Face with the proper filename
            dataset.push_to_hub(
                repo_id=repo_name,
                token=hf_token,
                config_name=f"{quarter}_{year}",
                commit_message=f"Add {quarter} {year} data"
            )
            
            logger.info(f"  ✅ Uploaded {csv_filename}")
            
            # Delete the temporary CSV file
            os.remove(TEMP_CSV)
            logger.info(f"  🗑️  Deleted {TEMP_CSV}")
            
            total_processed += dataset_count
            logger.info(f"  Progress: {total_processed:,}/{total_count:,} records ({total_processed/total_count*100:.1f}%)")
        
        logger.info(f"\n🎉 Upload complete! Processed {total_processed:,} records across {len(datasets)} quarterly files")
        logger.info(f"Dataset available at: https://huggingface.co/datasets/{repo_name}")
        
        # Validate uploaded data
        logger.info("\n=== VALIDATION ===")
        validate_uploaded_data(conn, repo_name, datasets, hf_token)
            
    except Exception as e:
        logger.error(f"Error during export/upload: {e}")
        # Clean up temp file if it exists
        if os.path.exists(TEMP_CSV):
            os.remove(TEMP_CSV)
            logger.info(f"Cleaned up {TEMP_CSV}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python export_and_upload_one_by_one.py <repo_name>")
        print("Example: python export_and_upload_one_by_one.py abigailhaddad/fedscope")
        sys.exit(1)
    
    repo_name = sys.argv[1]
    hf_token = os.getenv('HF_TOKEN') or os.getenv('HUGGINGFACE_HUB_TOKEN')
    
    # Don't require token if user is logged in via CLI
    export_and_upload_one_by_one(repo_name, hf_token)