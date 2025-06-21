#!/usr/bin/env python3
"""
Load only March 2020 data using the existing pipeline logic.
"""

import os
import logging
from load_to_duckdb_robust import load_single_dataset_robust

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Load March 2020 data using the main pipeline logic."""
    logger.info("Loading March 2020 data using main pipeline logic...")
    
    # Delete existing March 2020 data first
    import duckdb
    conn = duckdb.connect('fedscope_employment.duckdb')
    conn.execute("DELETE FROM employment_denormalized WHERE dataset_key = '2020_March'")
    logger.info("Deleted existing March 2020 data")
    conn.close()
    
    # Load March 2020 data
    conn = duckdb.connect('fedscope_employment.duckdb')
    dataset_path = "fedscope_data/extracted/FedScope_Employment_March_2020"
    dataset_key = "2020_March"
    month = "March"
    year = 2020
    
    success = load_single_dataset_robust(conn, dataset_path, dataset_key, month, year)
    conn.close()
    
    if success:
        logger.info("✅ Successfully loaded March 2020 data!")
        
        # Export to CSV
        logger.info("Exporting to CSV...")
        conn = duckdb.connect('fedscope_employment.duckdb', read_only=True)
        conn.execute("""
            COPY (
                SELECT * FROM employment_denormalized 
                WHERE dataset_key = '2020_March'
                ORDER BY dataset_key
            ) TO 'fedscope_employment_March_2020.csv' (HEADER, DELIMITER ',')
        """)
        conn.close()
        logger.info("Exported to fedscope_employment_March_2020.csv")
        
        # Upload to Hugging Face
        from huggingface_hub import HfApi
        api = HfApi()
        try:
            api.upload_file(
                path_or_fileobj="fedscope_employment_March_2020.csv",
                path_in_repo="fedscope_employment_March_2020.csv",
                repo_id="abigailhaddad/fedscope",
                repo_type="dataset"
            )
            logger.info("✅ Successfully uploaded to Hugging Face!")
        except Exception as e:
            logger.error(f"Failed to upload: {e}")
    else:
        logger.error("❌ Failed to load March 2020 data")

if __name__ == "__main__":
    main()