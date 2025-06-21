#!/usr/bin/env python3
"""
Complete script to fix March 2020 data:
1. Drop all existing March 2020 data from DuckDB tables
2. Load March 2020 using regular pipeline approach
3. Drop any March 2020 files from Hugging Face and upload the correct one
"""

import duckdb
import logging
from huggingface_hub import HfApi

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cleanup_march_2020_from_duckdb():
    """Remove all March 2020 data from all DuckDB tables."""
    logger.info("=== STEP 1: Cleaning up existing March 2020 data from DuckDB ===")
    
    conn = duckdb.connect('fedscope_employment.duckdb')
    
    # Get all tables
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    
    for (table_name,) in tables:
        try:
            # Check if table has dataset_key column
            columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'").fetchall()
            column_names = [col[0] for col in columns]
            
            if 'dataset_key' in column_names:
                # Delete March 2020 data
                result = conn.execute(f"DELETE FROM {table_name} WHERE dataset_key = '2020_March'")
                logger.info(f"  Deleted from {table_name}")
        except Exception as e:
            logger.warning(f"  Could not clean {table_name}: {e}")
    
    conn.close()
    logger.info("✅ DuckDB cleanup completed")

def load_march_2020_with_pipeline():
    """Load March 2020 using the regular pipeline approach."""
    logger.info("=== STEP 2: Loading March 2020 with pipeline approach ===")
    
    from load_to_duckdb_robust import load_single_dataset_robust, create_denormalized_records_for_dataset, load_lookup_tables
    import os
    
    conn = duckdb.connect('fedscope_employment.duckdb')
    
    # Set up paths
    dataset_path = "fedscope_data/extracted/FedScope_Employment_March_2020"
    dataset_key = "2020_March"
    month = "March"
    year = 2020
    
    # Load lookup tables first (like the main pipeline does)
    logger.info("Loading lookup tables for March 2020...")
    data_dirs = []
    for root, dirs, files in os.walk(dataset_path):
        if any(f.upper().startswith('DT') for f in files):
            data_dirs.append(root)
    
    if data_dirs:
        load_lookup_tables(conn, data_dirs[0], dataset_key)
        logger.info("✅ Lookup tables loaded")
    else:
        logger.error("❌ No lookup tables found")
        conn.close()
        return False
    
    # Load the raw data
    logger.info("Loading raw March 2020 data...")
    records = load_single_dataset_robust(conn, dataset_path, dataset_key, month, year)
    
    if records == 0:
        logger.error("❌ Failed to load raw data")
        conn.close()
        return False
    
    # Check raw data was loaded
    raw_count = conn.execute("SELECT COUNT(*) FROM employment_facts_all WHERE dataset_key = '2020_March'").fetchone()[0]
    logger.info(f"Raw data loaded: {raw_count:,} records")
    
    # Create denormalized records
    logger.info("Creating denormalized records...")
    create_denormalized_records_for_dataset(conn, dataset_key)
    
    # Verify denormalized data
    denorm_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = '2020_March'").fetchone()[0]
    logger.info(f"Denormalized data created: {denorm_count:,} records")
    
    if denorm_count == 0:
        logger.error("❌ Denormalization failed")
        conn.close()
        return False
    
    # Basic validation - check key fields have descriptions
    validation_fields = [
        ('agelvl', 'agelvlt', 'age level'),
        ('patco', 'patcot', 'PATCO category'),
        ('occ', 'occt', 'occupation'),
        ('loc', 'loct', 'location')
    ]
    
    logger.info("Validating key lookup joins...")
    validation_passed = True
    
    for code_field, desc_field, field_name in validation_fields:
        null_count = conn.execute(f"""
            SELECT COUNT(*) FROM employment_denormalized 
            WHERE dataset_key = '2020_March' AND {code_field} IS NOT NULL AND {desc_field} IS NULL
        """).fetchone()[0]
        
        if null_count > denorm_count * 0.1:  # More than 10% missing
            logger.error(f"❌ {field_name} lookup failed: {null_count:,} records missing descriptions")
            validation_passed = False
        else:
            logger.info(f"✅ {field_name} lookup: OK")
    
    conn.close()
    
    if validation_passed:
        logger.info("✅ March 2020 data loaded and validated successfully")
        return True
    else:
        logger.error("❌ Validation failed")
        return False

def export_and_upload_march_2020():
    """Export March 2020 to CSV and upload to Hugging Face."""
    logger.info("=== STEP 3: Exporting and uploading to Hugging Face ===")
    
    # Export to CSV
    logger.info("Exporting March 2020 to CSV...")
    conn = duckdb.connect('fedscope_employment.duckdb', read_only=True)
    
    conn.execute("""
        COPY (
            SELECT * FROM employment_denormalized 
            WHERE dataset_key = '2020_March'
            ORDER BY dataset_key
        ) TO 'fedscope_employment_March_2020.csv' (HEADER, DELIMITER ',')
    """)
    
    conn.close()
    logger.info("✅ Exported to fedscope_employment_March_2020.csv")
    
    # Upload to Hugging Face
    logger.info("Uploading to Hugging Face...")
    api = HfApi()
    
    try:
        # Check if file already exists and delete it first
        try:
            repo_files = api.list_repo_files(repo_id="abigailhaddad/fedscope", repo_type="dataset")
            march_files = [f for f in repo_files if "March_2020" in f]
            
            for file in march_files:
                logger.info(f"Deleting existing file: {file}")
                api.delete_file(
                    path_in_repo=file,
                    repo_id="abigailhaddad/fedscope",
                    repo_type="dataset"
                )
        except Exception as e:
            logger.info(f"No existing March 2020 files to delete (or error checking): {e}")
        
        # Upload the new file
        api.upload_file(
            path_or_fileobj="fedscope_employment_March_2020.csv",
            path_in_repo="fedscope_employment_March_2020.csv",
            repo_id="abigailhaddad/fedscope",
            repo_type="dataset"
        )
        
        logger.info("✅ Successfully uploaded to Hugging Face")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload to Hugging Face: {e}")
        return False

def final_validation():
    """Final validation that everything worked."""
    logger.info("=== STEP 4: Final validation ===")
    
    conn = duckdb.connect('fedscope_employment.duckdb', read_only=True)
    
    # Check record count
    march_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = '2020_March'").fetchone()[0]
    logger.info(f"Final March 2020 record count: {march_count:,}")
    
    # Compare with raw file
    try:
        with open("fedscope_data/extracted/FedScope_Employment_March_2020/FACTDATA_MAR2020.TXT", 'r') as f:
            raw_count = sum(1 for line in f) - 1  # Subtract header
        
        if march_count == raw_count:
            logger.info(f"✅ Record count matches raw file: {march_count:,}")
        else:
            logger.warning(f"⚠️ Record count difference: DB={march_count:,}, Raw={raw_count:,}")
    except Exception as e:
        logger.warning(f"Could not check raw file: {e}")
    
    # Sample validation
    sample = conn.execute("""
        SELECT agelvl, agelvlt, patco, patcot, occ, occt 
        FROM employment_denormalized 
        WHERE dataset_key = '2020_March' 
        LIMIT 3
    """).fetchall()
    
    logger.info("Sample records:")
    for i, (agelvl, agelvlt, patco, patcot, occ, occt) in enumerate(sample, 1):
        logger.info(f"  {i}. Age: {agelvl}->{agelvlt}, PATCO: {patco}->{patcot}, Occ: {occ}->{occt}")
    
    conn.close()
    
    return march_count > 0

def main():
    """Main function to completely fix March 2020 data."""
    logger.info("🚀 Starting complete March 2020 data fix...")
    
    try:
        # Step 1: Clean up existing data
        cleanup_march_2020_from_duckdb()
        
        # Step 2: Load with pipeline
        if not load_march_2020_with_pipeline():
            logger.error("❌ Pipeline load failed")
            return
        
        # Step 3: Export and upload
        if not export_and_upload_march_2020():
            logger.error("❌ Upload failed")
            return
        
        # Step 4: Final validation
        if final_validation():
            logger.info("🎉 SUCCESS: March 2020 data has been completely fixed!")
        else:
            logger.error("❌ Final validation failed")
            
    except Exception as e:
        logger.error(f"❌ Script failed with error: {e}")
        raise

if __name__ == "__main__":
    main()