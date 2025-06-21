#!/usr/bin/env python3
"""
Main orchestration script for FedScope employment data pipeline.

Usage:
    python main.py                    # Run extract and load steps (default)
    python main.py --extract          # Extract zip files only
    python main.py --load-duckdb      # Load data into DuckDB only
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
    
    # If no arguments, run extract and load-duckdb (but not HF upload)
    if len(sys.argv) == 1:
        args.extract = True
        args.load_duckdb = True
    
    try:
        if args.all or args.extract:
            logger.info("=== STEP 1: Extracting zip files ===")
            from fix_and_extract import main as extract_main
            extract_main()
        
        if args.all or args.load_duckdb:
            logger.info("\n=== STEP 2: Loading data into DuckDB ===")
            from load_to_duckdb_robust import load_all_data_robust
            load_all_data_robust()
            
            logger.info("\n=== STEP 3: Validating DuckDB database ===")
            from validate_duckdb import main as validate_main
            validate_main()
        
        logger.info("\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()