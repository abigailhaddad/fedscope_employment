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

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"
TEMP_CSV = "temp_fedscope_quarter.csv"

def validate_uploaded_data(conn, repo_name, datasets, hf_token):
    """Validate that uploaded CSV files match DuckDB source."""
    logger.info("Validating uploaded CSV files against DuckDB source...")
    
    try:
        # Get DuckDB totals by quarter
        duckdb_totals = {}
        expected_files = {}
        for dataset_key, year, quarter in datasets:
            count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = ?", [dataset_key]).fetchone()[0]
            csv_filename = f"fedscope_employment_{quarter}_{year}.csv"
            duckdb_totals[csv_filename] = count
            expected_files[csv_filename] = f"{quarter}_{year}"
        
        logger.info(f"DuckDB has {len(duckdb_totals)} quarterly datasets")
        
        # Check what's actually uploaded to HF
        api = HfApi(token=hf_token)
        uploaded_files = api.list_repo_files(repo_id=repo_name, repo_type="dataset")
        csv_files = [f for f in uploaded_files if f.endswith('.csv')]
        
        logger.info(f"Hugging Face has {len(csv_files)} CSV files")
        
        # Validate each file exists and report basic info
        validation_errors = []
        validated_count = 0
        
        for csv_filename, expected_count in duckdb_totals.items():
            if csv_filename in csv_files:
                logger.info(f"✅ {csv_filename}: Found on Hugging Face (expected {expected_count:,} records)")
                validated_count += 1
            else:
                error_msg = f"❌ {csv_filename}: Not found on Hugging Face"
                logger.error(error_msg)
                validation_errors.append(error_msg)
        
        # Check for unexpected files
        unexpected_files = [f for f in csv_files if f not in duckdb_totals.keys()]
        if unexpected_files:
            logger.warning(f"Found {len(unexpected_files)} unexpected CSV files:")
            for f in unexpected_files:
                logger.warning(f"  {f}")
        
        # Summary
        logger.info(f"\n=== VALIDATION SUMMARY ===")
        logger.info(f"Expected CSV files: {len(duckdb_totals)}")
        logger.info(f"Found CSV files: {validated_count}")
        logger.info(f"Total expected records: {sum(duckdb_totals.values()):,}")
        
        if validation_errors:
            logger.error(f"Validation failed! {len(validation_errors)} errors:")
            for error in validation_errors:
                logger.error(f"  {error}")
        else:
            logger.info("🎉 All CSV files uploaded successfully!")
            
        # Print dataset info
        logger.info(f"\n=== DATASET INFO ===")
        logger.info(f"Repository: https://huggingface.co/datasets/{repo_name}")
        logger.info(f"Time range: {min(year for _, year, _ in datasets)} - {max(year for _, year, _ in datasets)}")
        logger.info(f"CSV files: {len(datasets)}")
        logger.info(f"Total records: {sum(duckdb_totals.values()):,}")
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}")

def upload_dataset_card(repo_name):
    """Upload dataset_card.md as README.md to Hugging Face."""
    logger.info("Uploading dataset card as README...")
    
    try:
        # Check if dataset_card.md exists
        dataset_card_path = "dataset_card.md"
        if not os.path.exists(dataset_card_path):
            logger.error(f"Dataset card not found: {dataset_card_path}")
            return
        
        # Upload as README.md
        api = HfApi()
        api.upload_file(
            path_or_fileobj=dataset_card_path,
            path_in_repo="README.md",
            repo_id=repo_name,
            repo_type="dataset",
            commit_message="Add dataset card as README"
        )
        
        logger.info("✅ Dataset card uploaded successfully as README.md")
        
    except Exception as e:
        logger.error(f"Failed to upload dataset card: {e}")

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
        
        # Check and clear existing files in repository (except .gitattributes)
        logger.info("Checking repository contents before upload...")
        try:
            existing_files = api.list_repo_files(repo_id=repo_name, repo_type="dataset")
            files_to_delete = [f for f in existing_files if f != '.gitattributes']
            
            if files_to_delete:
                logger.warning(f"Found {len(files_to_delete)} existing files in repository:")
                for f in files_to_delete:
                    logger.warning(f"  - {f}")
                
                # Ask for confirmation to proceed
                response = input(f"\n⚠️  Repository contains {len(files_to_delete)} files. Delete them and proceed? (y/N): ")
                if response.lower() != 'y':
                    logger.info("Upload cancelled by user")
                    return
                
                logger.info("Deleting existing files...")
                for file_path in files_to_delete:
                    logger.info(f"  Deleting {file_path}...")
                    api.delete_file(
                        path_in_repo=file_path,
                        repo_id=repo_name,
                        repo_type="dataset",
                        token=hf_token,
                        commit_message=f"Delete {file_path} before fresh upload"
                    )
                
                # Verify deletion worked
                logger.info("Verifying deletion...")
                remaining_files = api.list_repo_files(repo_id=repo_name, repo_type="dataset")
                remaining_data_files = [f for f in remaining_files if f != '.gitattributes']
                
                if remaining_data_files:
                    logger.error(f"❌ Deletion failed! {len(remaining_data_files)} files still remain:")
                    for f in remaining_data_files:
                        logger.error(f"  - {f}")
                    logger.error("Stopping upload to prevent conflicts")
                    return
                else:
                    logger.info("✅ Repository successfully cleared")
            else:
                logger.info("✅ Repository is clean, proceeding with upload")
        except Exception as e:
            logger.error(f"Could not check/clear repository: {e}")
            logger.error("Stopping upload for safety")
            return
        
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
            
            # Upload CSV file directly without dataset conversion
            logger.info(f"  Uploading {csv_filename} directly as CSV...")
            
            # Upload the CSV file directly using the API
            api.upload_file(
                path_or_fileobj=TEMP_CSV,
                path_in_repo=csv_filename,
                repo_id=repo_name,
                repo_type="dataset",
                token=hf_token,
                commit_message=f"Add {quarter} {year} data"
            )
            
            logger.info(f"  ✅ Uploaded {csv_filename}")
            
            # Delete the temporary CSV file
            os.remove(TEMP_CSV)
            logger.info(f"  🗑️  Deleted {TEMP_CSV}")
            
            total_processed += dataset_count
            logger.info(f"  Progress: {total_processed:,}/{total_count:,} records ({total_processed/total_count*100:.1f}%)")
        
        logger.info(f"\n🎉 Upload complete! Processed {total_processed:,} records across {len(datasets)} quarterly files")
        
        # Upload dataset card as README
        logger.info(f"\n=== UPLOADING DATASET CARD ===")
        upload_dataset_card(repo_name)
        
        logger.info(f"\nDataset available at: https://huggingface.co/datasets/{repo_name}")
        
        # Validate uploaded data
        logger.info(f"\n=== VALIDATION ===")
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