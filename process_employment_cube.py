#!/usr/bin/env python3
"""
Employment Cube Data Pipeline

Processes all employment cube data:
1. Extracts zip files from employment_cube/raw/
2. Creates parquet files in employment_cube/parquet/
3. Validates the output

Usage:
    python process_employment_cube.py
"""

import sys
from pathlib import Path

# Add code directory to path for imports
code_dir = Path("code")
if str(code_dir) not in sys.path:
    sys.path.insert(0, str(code_dir))

from fedscope_utils import (
    setup_logging, setup_code_path, extract_zip_files, 
    validate_parquet_files, collect_documentation_files
)

logger = setup_logging()

def run_extraction():
    """Extract all employment zip files"""
    logger.info("=== EXTRACTING EMPLOYMENT CUBE ZIP FILES ===")
    extract_zip_files("employment_cube/raw", "employment_cube/extracted")

def run_parquet_creation():
    """Create parquet files from extracted employment data"""
    logger.info("=== CREATING EMPLOYMENT CUBE PARQUET FILES ===")
    
    setup_code_path()
    
    try:
        from process_employment_cube import process_all_datasets
        
        # Call with the correct directories for employment cube data
        results = process_all_datasets(
            extracted_dir='employment_cube/extracted',
            parquet_dir='employment_cube/parquet'
        )
        
        if results:
            logger.info(f"Successfully created {len(results)} parquet files")
        else:
            logger.warning("No parquet files were created")
            
    except ImportError as e:
        logger.error(f"Could not import text_to_parquet: {e}")
        logger.info("You may need to run this from the repository root directory")
    except Exception as e:
        logger.error(f"Error creating parquet files: {e}")

def run_validation():
    """Validate parquet files"""
    logger.info("=== VALIDATING EMPLOYMENT CUBE PARQUET FILES ===")
    validate_parquet_files("employment_cube/parquet")

def collect_documentation():
    """Collect documentation files"""
    logger.info("=== COLLECTING DOCUMENTATION PDF FILES ===")
    collect_documentation_files(
        source_dir="employment_cube/extracted",
        target_dir="employment_cube/documentation_pdfs",
        pipeline_name="Employment Cube"
    )

def main():
    """Run the complete employment cube pipeline"""
    try:
        logger.info("=== STARTING EMPLOYMENT CUBE PIPELINE ===")
        
        run_extraction()
        run_parquet_creation()
        run_validation()
        collect_documentation()
        
        logger.info("=== EMPLOYMENT CUBE PIPELINE COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()