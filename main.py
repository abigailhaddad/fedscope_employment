#!/usr/bin/env python3
"""
Main orchestration script for FedScope employment data pipeline.

Usage:
    python main.py --extract          # Extract zip files
    python main.py --load-duckdb      # Load data into DuckDB
    python main.py --all              # Run extract and load steps
"""

import argparse
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='FedScope Employment Data Pipeline')
    parser.add_argument('--extract', action='store_true', help='Extract zip files')
    parser.add_argument('--load-duckdb', action='store_true', help='Load data into DuckDB')
    parser.add_argument('--all', action='store_true', help='Run entire pipeline')
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    try:
        if args.all or args.extract:
            logger.info("=== STEP 1: Extracting zip files ===")
            from fix_and_extract import main as extract_main
            extract_main()
        
        if args.all or args.load_duckdb:
            logger.info("\n=== STEP 2: Loading data into DuckDB ===")
            from load_to_duckdb_robust import load_all_data_robust
            load_all_data_robust()
        
        logger.info("\n✅ Pipeline completed successfully!")
        
        if args.all:
            logger.info("\nTo upload to Hugging Face, run:")
            logger.info("python export_and_upload_one_by_one.py <your-repo-name>")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()