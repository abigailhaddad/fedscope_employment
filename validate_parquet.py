#!/usr/bin/env python3
"""
Validate Parquet files - check record counts, merges, and completeness.
"""

import pandas as pd
import os
import glob
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PARQUET_DIR = "fedscope_data/parquet"
EXTRACTED_DIR = "fedscope_data/extracted"

def validate_all_parquet_files():
    """Validate all parquet files."""
    
    # Check if parquet directory exists
    if not os.path.exists(PARQUET_DIR):
        logger.error(f"Parquet directory not found: {PARQUET_DIR}")
        return False
    
    # Get all parquet files
    parquet_files = sorted(glob.glob(os.path.join(PARQUET_DIR, "*.parquet")))
    
    if not parquet_files:
        logger.error(f"No parquet files found in {PARQUET_DIR}")
        return False
    
    logger.info(f"Found {len(parquet_files)} Parquet files to validate")
    
    total_records = 0
    failed_validations = []
    validation_summary = []
    
    for i, pf in enumerate(parquet_files, 1):
        filename = os.path.basename(pf)
        logger.info(f"\n[{i}/{len(parquet_files)}] Validating {filename}...")
        
        try:
            # Read parquet file
            df = pd.read_parquet(pf)
            record_count = len(df)
            total_records += record_count
            
            # Basic validation
            validation_result = validate_single_file(df, filename)
            validation_summary.append(validation_result)
            
            if not validation_result['valid']:
                failed_validations.append(filename)
                
            logger.info(f"  Records: {record_count:,}")
            logger.info(f"  Columns: {len(df.columns)}")
            
            # Check key fields
            key_fields = ['dataset_key', 'quarter', 'year', 'agelvlt', 'patcot', 'occt', 'loct']
            missing_fields = [f for f in key_fields if f not in df.columns]
            if missing_fields:
                logger.warning(f"  Missing key fields: {missing_fields}")
            
            # Check for null descriptions (merge failures) - all lookup tables
            null_checks = []
            lookup_fields = [
                'agelvlt', 'agysubt', 'edlvlt', 'loslvlt', 'loct', 'occfamt', 'occt', 
                'occtypt', 'patcot', 'payplant', 'ppgrdt', 'ppgroupt', 'sallvlt', 
                'stemocct', 'supervist', 'toat', 'wkstatt', 'wrkscht', 'wstypt'
            ]
            for field in lookup_fields:
                if field in df.columns:
                    null_count = df[field].isnull().sum()
                    null_pct = (null_count / record_count) * 100
                    null_checks.append((field, null_count, null_pct))
                    
                    if null_pct > 10:  # More than 10% null
                        logger.warning(f"  ⚠️  {field}: {null_count:,} null ({null_pct:.1f}%)")
                    elif null_pct > 0:
                        logger.info(f"  ⚠️  {field}: {null_count:,} null ({null_pct:.1f}%)")
                    else:
                        logger.info(f"  ✅ {field}: complete merge")
            
            validation_result['null_checks'] = null_checks
            
            # Check for fields that should be null in certain time periods
            year = df['year'].iloc[0] if 'year' in df.columns else None
            if year and year < 2016:
                # Pay plan fields should be null before 2016
                pp_fields = ['payplant', 'ppgrdt', 'ppgroupt']
                for field in pp_fields:
                    if field in df.columns:
                        non_null_count = df[field].notna().sum()
                        if non_null_count > 0:
                            logger.error(f"  ❌ {field}: {non_null_count:,} non-null values in {year} (should be null before 2016)")
                        else:
                            logger.info(f"  ✅ {field}: correctly null in {year}")
            
        except Exception as e:
            logger.error(f"  ❌ Error validating {filename}: {e}")
            failed_validations.append(filename)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total files: {len(parquet_files)}")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Failed validations: {len(failed_validations)}")
    
    if failed_validations:
        logger.error(f"Failed files: {failed_validations}")
    
    # Check expected file count (72 quarters from 1998-2024)
    expected_files = calculate_expected_files()
    logger.info(f"Expected files: {expected_files}")
    
    if len(parquet_files) == expected_files:
        logger.info("✅ File count matches expected")
    else:
        logger.warning(f"⚠️  File count mismatch: got {len(parquet_files)}, expected {expected_files}")
    
    # Check for data quality issues
    high_null_files = []
    for result in validation_summary:
        if result.get('null_checks'):
            for field, null_count, null_pct in result['null_checks']:
                if null_pct > 10:
                    high_null_files.append((result['filename'], field, null_pct))
    
    if high_null_files:
        logger.warning(f"\nFiles with high null rates (>10%):")
        for filename, field, pct in high_null_files:
            logger.warning(f"  {filename}: {field} ({pct:.1f}% null)")
    
    return len(failed_validations) == 0

def validate_single_file(df, filename):
    """Validate a single parquet file."""
    result = {
        'filename': filename,
        'valid': True,
        'issues': []
    }
    
    # Check required columns
    required_cols = ['dataset_key', 'quarter', 'year', 'employment']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        result['valid'] = False
        result['issues'].append(f"Missing required columns: {missing_cols}")
    
    # Check employment count (should be all 1s)
    if 'employment' in df.columns:
        non_one_employment = df[df['employment'] != '1']
        if len(non_one_employment) > 0:
            result['issues'].append(f"Found {len(non_one_employment)} records with employment != 1")
    
    # Check for completely empty description fields - all lookup tables
    lookup_fields = [
        'agelvlt', 'agysubt', 'edlvlt', 'loslvlt', 'loct', 'occfamt', 'occt', 
        'occtypt', 'patcot', 'payplant', 'ppgrdt', 'ppgroupt', 'sallvlt', 
        'stemocct', 'supervist', 'toat', 'wkstatt', 'wrkscht', 'wstypt'
    ]
    for field in lookup_fields:
        if field in df.columns:
            null_count = df[field].isnull().sum()
            if null_count == len(df):
                result['valid'] = False
                result['issues'].append(f"All {field} values are null - complete merge failure")
    
    return result

def calculate_expected_files():
    """Calculate expected number of files based on FedScope release schedule."""
    # 1998-2008: September only (11 files)
    # 2009: September and December (2 files)  
    # 2010-2023: March, June, September, December (14 * 4 = 56 files)
    # 2024: March, June, September so far (3 files)
    
    files_1998_2008 = 11  # Sept only
    files_2009 = 2       # Sept, Dec
    files_2010_2023 = 14 * 4  # 4 quarters * 14 years
    files_2024 = 3       # Mar, Jun, Sept
    
    return files_1998_2008 + files_2009 + files_2010_2023 + files_2024

if __name__ == "__main__":
    validate_all_parquet_files()