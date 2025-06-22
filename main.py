#!/usr/bin/env python3
"""
Main orchestration script for FedScope employment data pipeline.

Usage:
    python main.py                    # Run extract and create parquet files (default)
    python main.py --extract          # Extract zip files only
    python main.py --parquet          # Create parquet files only
    python main.py --validate         # Validate parquet files
"""

import argparse
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='FedScope Employment Data Pipeline')
    parser.add_argument('--extract', action='store_true', help='Extract zip files')
    parser.add_argument('--parquet', action='store_true', help='Create parquet files from extracted data')
    parser.add_argument('--validate', action='store_true', help='Validate parquet files')
    parser.add_argument('--sequential', action='store_true', help='Process parquet files sequentially (default: parallel)')
    
    args = parser.parse_args()
    
    # If no arguments, run extract and parquet
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and args.sequential):
        args.extract = True
        args.parquet = True
        args.validate = True
    
    try:
        if args.extract:
            logger.info("=== STEP 1: Extracting zip files ===")
            from rename_and_extract import main as extract_main
            extract_main()
        
        if args.parquet:
            logger.info("\n=== STEP 2: Creating Parquet files ===")
            from text_to_parquet import process_all_datasets
            results = process_all_datasets()
            
            if not results:
                logger.error("No parquet files were created!")
                sys.exit(1)
        
        if args.validate:
            logger.info("\n=== STEP 3: Validating Parquet files ===")
            from validate_parquet import validate_all_parquet_files
            validate_all_parquet_files()
        
        logger.info("\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()