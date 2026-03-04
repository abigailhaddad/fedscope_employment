#!/usr/bin/env python3
"""
Separations & Accessions Data Pipeline

Processes all separations and accessions data:
1. Extracts zip files from separations_accessions/raw/
2. Creates parquet files in separations_accessions/parquet/
3. Validates the output

Usage:
    python process_separations_accessions.py
"""

import sys
from pathlib import Path

# Add code directory to path for imports
code_dir = Path("code")
if str(code_dir) not in sys.path:
    sys.path.insert(0, str(code_dir))

import process_separations_accessions as pas
from fedscope_utils import (
    setup_logging, setup_code_path, extract_zip_files, 
    validate_parquet_files, collect_documentation_files
)

logger = setup_logging()

def run_extraction():
    """Extract all separations/accessions zip files"""
    logger.info("=== EXTRACTING SEPARATIONS & ACCESSIONS ZIP FILES ===")
    extract_zip_files("separations_accessions/raw", "separations_accessions/extracted")

def run_parquet_creation():
    """Create parquet files from extracted separations/accessions data"""
    logger.info("=== CREATING SEPARATIONS & ACCESSIONS PARQUET FILES ===")
    
    try:
        # Update the module's directory constants for new structure
        pas.CHANGES_DIR = "separations_accessions/raw"
        pas.EXTRACTED_DIR = "separations_accessions/extracted"
        pas.PARQUET_DIR = "separations_accessions/parquet"
        
        # Run the processing function directly
        results = pas.process_all_accessions_separations()
        
        if results:
            logger.info(f"Successfully processed {len(results)} files:")
            for result in results:
                logger.info(f"  {result['filename']}")
        else:
            logger.warning("No files were processed")
        
        logger.info("Separations & accessions parquet creation complete")
        
    except Exception as e:
        logger.error(f"Error creating parquet files: {e}")

def run_validation():
    """Validate parquet files"""
    logger.info("=== VALIDATING SEPARATIONS & ACCESSIONS PARQUET FILES ===")
    validate_parquet_files("separations_accessions/parquet", ['agysubt', 'occt', 'efdate'])

def collect_documentation():
    """Collect documentation files"""
    logger.info("=== COLLECTING DOCUMENTATION PDF FILES ===")
    collect_documentation_files(
        source_dir="separations_accessions/extracted",
        target_dir="separations_accessions/documentation_pdfs",
        pipeline_name="Separations & Accessions"
    )

def main():
    """Run the complete separations & accessions pipeline"""
    try:
        logger.info("=== STARTING SEPARATIONS & ACCESSIONS PIPELINE ===")
        
        run_extraction()
        run_parquet_creation()
        run_validation()
        collect_documentation()
        
        logger.info("=== SEPARATIONS & ACCESSIONS PIPELINE COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()