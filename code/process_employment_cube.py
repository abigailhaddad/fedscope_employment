#!/usr/bin/env python3
"""
Convert TXT files to Parquet using exact same logic as load_to_duckdb_robust.py
"""

import pandas as pd
import os
import re
import glob
from pathlib import Path
import logging
from fedscope_utils import DuplicateLogger, load_lookup_tables, create_denormalized_records, get_quarter_year_from_filename

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXTRACTED_DIR = "fedscope_data/extracted"
PARQUET_DIR = "fedscope_data/parquet"




def process_single_dataset(dataset_dir, extracted_dir=None, parquet_dir=None, duplicate_logger=None):
    """Process a single dataset directory to create a Parquet file."""
    try:
        # Use provided directories or fall back to defaults
        extract_dir = extracted_dir or EXTRACTED_DIR
        output_dir = parquet_dir or PARQUET_DIR
        
        month, year = get_quarter_year_from_filename(dataset_dir)
        if not month or not year:
            logger.warning(f"Could not determine period for {dataset_dir}, skipping")
            return None
        
        dataset_key = f"{year}_{month}"
        dataset_path = os.path.join(extract_dir, dataset_dir)
        
        logger.info(f"Processing {dataset_key}...")
        
        # Find the data directory containing files
        data_dirs = []
        for root, dirs, files in os.walk(dataset_path):
            if any(f.upper().startswith('FACTDATA') for f in files):
                data_dirs.append(root)
            elif dataset_key == '2025_March' and any(f.startswith('March_2025_Employment_') for f in files):
                data_dirs.append(root)
        
        if not data_dirs:
            logger.warning(f"  No data files found in {dataset_path}")
            return None
        
        data_dir = data_dirs[0]
        
        # Find fact file or March 2025 employment files
        fact_files = glob.glob(os.path.join(data_dir, "FACTDATA*.TXT"))
        march_2025_files = glob.glob(os.path.join(data_dir, "March_2025_Employment_*.txt"))
        if not march_2025_files:
            # Try Status Employment pattern for March 2025
            march_2025_files = glob.glob(os.path.join(data_dir, "Status Employment*.txt"))
        
        if not fact_files and not march_2025_files:
            logger.warning(f"  No fact data files found")
            return None
        
        # Handle March 2025 special case
        if march_2025_files and dataset_key == '2025_March':
            logger.info(f"  Processing March 2025 format with {len(march_2025_files)} files")
            return process_march_2025_data(data_dir, dataset_key, month, year, output_dir, duplicate_logger)
        
        fact_file = fact_files[0]
        
        # Load fact data
        logger.info(f"  Loading fact data from {os.path.basename(fact_file)}...")
        fact_df = pd.read_csv(fact_file, sep=',', encoding='latin-1', dtype=str)
        
        # Clean column names
        fact_df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in fact_df.columns]
        
        # Add metadata columns
        fact_df.insert(0, 'dataset_key', dataset_key)
        fact_df.insert(1, 'quarter', month)
        fact_df.insert(2, 'year', year)
        
        logger.info(f"  Loaded {len(fact_df):,} records")
        
        # Load lookup tables
        lookups = load_lookup_tables(data_dir, dataset_key, duplicate_logger)
        
        # Create denormalized records
        logger.info(f"  Creating denormalized records...")
        denormalized_df = create_denormalized_records(fact_df, lookups, dataset_key)
        
        # Create output filename
        output_filename = f"fedscope_employment_{month}_{year}.parquet"
        output_path = os.path.join(output_dir, output_filename)
        
        # Write to Parquet with compression
        logger.info(f"  Writing to {output_filename}...")
        denormalized_df.to_parquet(output_path, compression='zstd', index=False)
        
        # Get file size
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"  ✅ Created {output_filename} ({size_mb:.1f} MB, {len(denormalized_df):,} records)")
        
        return {
            'dataset_key': dataset_key,
            'filename': output_filename,
            'records': len(denormalized_df),
            'size_mb': size_mb
        }
        
    except Exception as e:
        logger.error(f"Error processing {dataset_dir}: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_march_2025_data(data_dir, dataset_key, month, year, parquet_dir, duplicate_logger):
    """Special processing for March 2025 data format."""
    try:
        # For March 2025, we need to find files across all Part directories
        # Get the parent directory to search all FedScope_Employment_March_2025* folders
        parent_dir = os.path.dirname(data_dir)
        
        # Find all March 2025 employment files across all Part directories
        employment_files = []
        for part_dir in sorted(glob.glob(os.path.join(parent_dir, "FedScope_Employment_March_2025*"))):
            part_files = glob.glob(os.path.join(part_dir, "March_2025_Employment_*.txt"))
            employment_files.extend(part_files)
        
        employment_files = sorted(employment_files)
        
        if not employment_files:
            logger.error("No March 2025 employment files found")
            return None
            
        logger.info(f"  Found {len(employment_files)} March 2025 employment files")
        
        # Load and combine all employment files
        dfs = []
        for emp_file in employment_files:
            logger.info(f"  Loading {os.path.basename(emp_file)}...")
            # March 2025 format uses pipe delimiter with quotes
            df = pd.read_csv(emp_file, sep='|', quotechar='"', dtype=str, encoding='latin-1')
            dfs.append(df)
            logger.info(f"    Loaded {len(df):,} records")
        
        # Combine all dataframes
        logger.info("  Combining all employment files...")
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"  Combined {len(combined_df):,} total records")
        
        # Get a reference dataframe for column structure
        ref_parquet = os.path.join(parquet_dir, 'fedscope_employment_September_2024.parquet')
        if os.path.exists(ref_parquet):
            ref_df = pd.read_parquet(ref_parquet)
            target_columns = list(ref_df.columns)
        else:
            # Fallback to expected columns
            logger.warning("  Could not find reference parquet for column structure")
            target_columns = None
        
        # Create standardized dataframe
        new_df = pd.DataFrame()
        
        # Map March 2025 columns to historical format
        column_mapping = {
            'AGY': 'agy',
            'AGYSUB': 'agysub', 
            'LOC': 'loc',
            'AGELVLT': 'agelvlt',
            'EDLVL': 'edlvl',
            'EDLVLT': 'edlvlt',
            'LOS': 'los',
            'OCC': 'occ',
            'OCCFAM': 'occfam',
            'OCCFAMT': 'occfamt',
            'OCCT': 'occt',
            'PAYPLAN': 'payplan',
            'PAYPLANT': 'payplant',
            'SUPERVIS': 'supervis',
            'SUPERVIST': 'supervist',
            'TOA': 'toa',
            'TOAT': 'toat',
            'WORKSCH': 'worksch',
            'WORKSCHT': 'wrkscht',
            'DATECODE': 'datecode',
            'COUNT': 'employment',
            'SALARY': 'salary',
            'AGYSUBT': 'agysubt',
            'AGYT': 'agyt',
            'STATET': 'loct'
        }
        
        # Copy mapped columns
        for march_col, hist_col in column_mapping.items():
            if march_col in combined_df.columns:
                new_df[hist_col] = combined_df[march_col]
        
        # Add metadata columns
        new_df.insert(0, 'dataset_key', dataset_key)
        new_df.insert(1, 'quarter', month)
        new_df.insert(2, 'year', year)
        
        # Fill missing columns with empty strings
        if target_columns:
            for col in target_columns:
                if col not in new_df.columns:
                    new_df[col] = ''
            # Reorder to match target
            new_df = new_df[target_columns]
        
        logger.info(f"  Standardized to {len(new_df.columns)} columns")
        
        # Note: For March 2025, lookups are already merged, so we skip the lookup joining
        # The duplicate logger won't find any duplicates since lookups are pre-joined
        
        # Save to parquet - use the parquet directory passed from process_single_dataset
        output_filename = f"fedscope_employment_{month}_{year}.parquet"
        output_path = os.path.join(parquet_dir, output_filename)
        
        logger.info(f"  Saving to {output_filename}...")
        new_df.to_parquet(output_path, compression='zstd', index=False)
        
        # Get file size
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"  Created {output_filename} ({size_mb:.1f} MB)")
        
        return {
            'dataset_key': dataset_key,
            'filename': output_filename,
            'records': len(new_df),
            'size_mb': size_mb
        }
        
    except Exception as e:
        logger.error(f"Error processing March 2025 data: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_all_datasets(extracted_dir=None, parquet_dir=None):
    """Process all datasets to create Parquet files."""
    # Use provided directories or fall back to defaults
    extract_dir = extracted_dir or EXTRACTED_DIR
    output_dir = parquet_dir or PARQUET_DIR
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Create duplicate logger for employment cube (saves to employment_cube directory)
    dup_logger = DuplicateLogger(log_dir="employment_cube")
    
    # Get all dataset directories
    dataset_dirs = sorted([d for d in os.listdir(extract_dir) 
                          if os.path.isdir(os.path.join(extract_dir, d))])
    
    logger.info(f"Found {len(dataset_dirs)} datasets to process")
    
    results = []
    
    # Sequential processing (keep it simple)
    for i, dataset_dir in enumerate(dataset_dirs, 1):
        logger.info(f"\n[{i}/{len(dataset_dirs)}] Processing {dataset_dir}...")
        result = process_single_dataset(dataset_dir, extract_dir, output_dir, dup_logger)
        if result:
            results.append(result)
    
    # Save duplicate logs
    dup_logger.save_logs()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("PROCESSING COMPLETE")
    logger.info("="*60)
    logger.info(f"Successfully processed: {len(results)}/{len(dataset_dirs)} datasets")
    
    if results:
        total_records = sum(r['records'] for r in results)
        total_size = sum(r['size_mb'] for r in results)
        logger.info(f"Total records: {total_records:,}")
        logger.info(f"Total size: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
        logger.info(f"Average file size: {total_size/len(results):.1f} MB")
    
    return results

if __name__ == "__main__":
    import sys
    
    # Check if extracted data exists
    if not os.path.exists(EXTRACTED_DIR):
        logger.error(f"Extracted data directory not found: {EXTRACTED_DIR}")
        logger.info("Please run fix_and_extract.py first")
        sys.exit(1)
    
    process_all_datasets()