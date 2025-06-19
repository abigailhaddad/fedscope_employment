#!/usr/bin/env python3
"""
Main orchestration script for FedScope employment data pipeline.

Usage:
    python main.py --extract          # Extract zip files
    python main.py --load-duckdb      # Load data into DuckDB
    python main.py --export-postgres  # Export from DuckDB to PostgreSQL
    python main.py --export-huggingface  # Export to Hugging Face format
    python main.py --all              # Run entire pipeline
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
    parser.add_argument('--export-huggingface', action='store_true', help='Export to Hugging Face')
    parser.add_argument('--push-to-hub', action='store_true', help='Push dataset to Hugging Face Hub')
    parser.add_argument('--repo-name', type=str, help='Hugging Face repository name')
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
        
        if args.all or args.export_huggingface:
            logger.info("\n=== STEP 3: Exporting to Hugging Face ===")
            from export_to_huggingface import export_to_huggingface
            export_to_huggingface(
                push_to_hub=args.push_to_hub,
                repo_name=args.repo_name
            )
        
        logger.info("\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()