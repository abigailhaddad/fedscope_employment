#!/usr/bin/env python3
"""
Fixed version: Process accessions and separations data from fedscope_changes directory.

This script properly handles both formats:
1. March 2025 data: pipe-separated with lookups already merged
2. FY data: comma-separated with only codes (needs lookup merging)
"""

import os
import zipfile
import pandas as pd
import glob
import logging
from pathlib import Path
import re
from fedscope_utils import load_lookup_tables, create_denormalized_records, DuplicateLogger

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CHANGES_DIR = "fedscope_changes"
EXTRACTED_DIR = "fedscope_data/extracted"
PARQUET_DIR = "fedscope_data/parquet"

def identify_data_type(file_list):
    """Identify whether this is accessions or separations data."""
    for file_name in file_list:
        if 'Accessions' in file_name or 'ACCDATA' in file_name.upper():
            return 'accessions'
        elif 'Separations' in file_name or 'SEPDATA' in file_name.upper():
            return 'separations'
    return None

def extract_time_period(file_list):
    """Extract time period from file names."""
    for file_name in file_list:
        # Look for patterns like "March_2025_Separations.txt" or similar
        match = re.search(r'(March|June|September|December)[_\s]*(\d{4})', file_name)
        if match:
            month, year = match.groups()
            return month, int(year)
        
        # Look for patterns like "FY2015-2019" in documentation
        match = re.search(r'FY(\d{4})-(\d{4})', file_name)
        if match:
            start_year, end_year = match.groups()
            return f"FY{start_year}-{end_year}", None
    
    return None, None

def process_fy_data(df, extract_dir, dataset_key, data_type, duplicate_logger=None):
    """Process FY format data - already parsed correctly, just need to merge lookups."""
    # The FY data is already properly formatted with separate columns
    # Just need to clean column names and merge lookups
    
    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    
    # Add dataset_key
    df['dataset_key'] = dataset_key
    
    # Clean column values
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    # Load lookup tables and merge
    lookups = load_lookup_tables(extract_dir, dataset_key, duplicate_logger)
    
    # Create denormalized records using existing logic
    logger.info(f"  Merging lookup tables...")
    denormalized_df = create_denormalized_records(df, lookups, dataset_key)
    
    # Make sure we return all columns from denormalized_df
    return denormalized_df

def process_march_2025_data(df):
    """Process March 2025 format data - already has lookups merged."""
    # March 2025 data already has the descriptive fields, just need to clean it
    
    # Clean column names (remove quotes if present and make lowercase)
    df.columns = [col.strip('"').lower() for col in df.columns]
    
    # Clean string values
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip('"').str.strip()
    
    return df

def process_accessions_separations_zip(zip_path, duplicate_logger=None):
    """Process a single accessions/separations ZIP file."""
    try:
        logger.info(f"Processing {os.path.basename(zip_path)}...")
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            file_list = zf.namelist()
            
            # Identify data type
            data_type = identify_data_type(file_list)
            if not data_type:
                logger.warning(f"  Could not identify data type for {zip_path}")
                return None
            
            # Extract time period
            month, year = extract_time_period(file_list)
            if not month:
                logger.warning(f"  Could not identify time period for {zip_path}")
                return None
            
            # Create extraction directory
            if year:
                extract_dir = os.path.join(EXTRACTED_DIR, f"FedScope_{data_type.title()}_{month}_{year}")
                dataset_key = f"{year}_{month}_{data_type}"
            else:
                extract_dir = os.path.join(EXTRACTED_DIR, f"FedScope_{data_type.title()}_{month}")
                dataset_key = f"{month}_{data_type}"
            
            logger.info(f"  Extracting to {extract_dir}...")
            
            # Extract all files
            zf.extractall(extract_dir)
            
            # Find the main data file
            main_data_files = []
            for file_name in file_list:
                # Look for main data files
                if 'March_2025' in file_name and file_name.endswith('.txt'):
                    main_data_files.append(file_name)
                elif (file_name.upper().startswith('ACCDATA') or 
                      file_name.upper().startswith('SEPDATA')) and file_name.endswith('.TXT'):
                    main_data_files.append(file_name)
            
            if not main_data_files:
                logger.warning(f"  No main data file found for {data_type}")
                return None
            
            main_data_file = main_data_files[0]
            main_data_path = os.path.join(extract_dir, main_data_file)
            
            # Load main data
            logger.info(f"  Loading main data from {main_data_file}...")
            
            # Determine format and load accordingly
            if 'March_2025' in main_data_file:
                # March 2025 format: pipe-separated with quotes
                df = pd.read_csv(main_data_path, sep='|', encoding='latin-1', dtype=str, quotechar='"')
                logger.info(f"  Loaded {len(df):,} records (March 2025 format)")
                
                # Process March 2025 data
                df = process_march_2025_data(df)
                
                # Add metadata columns
                df['dataset_key'] = dataset_key
                df['data_type'] = data_type
                if year:
                    df['quarter'] = month
                    df['year'] = year
                    
                denormalized_df = df
                
            else:
                # FY format: comma-separated without quotes
                df = pd.read_csv(main_data_path, sep=',', encoding='latin-1', dtype=str)
                logger.info(f"  Loaded {len(df):,} records (FY format)")
                
                # Process FY data (parse and merge lookups)
                denormalized_df = process_fy_data(df, extract_dir, dataset_key, data_type, duplicate_logger)
                
                # Add metadata columns (only if they don't exist)
                if 'data_type' not in denormalized_df.columns:
                    denormalized_df['data_type'] = data_type
                if year:
                    if 'quarter' not in denormalized_df.columns:
                        denormalized_df['quarter'] = month
                    if 'year' not in denormalized_df.columns:
                        denormalized_df['year'] = year
            
            # Create output filename
            if year:
                output_filename = f"fedscope_{data_type}_{month}_{year}.parquet"
            else:
                output_filename = f"fedscope_{data_type}_{month}.parquet"
            
            output_path = os.path.join(PARQUET_DIR, output_filename)
            
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
                'size_mb': size_mb,
                'data_type': data_type
            }
            
    except FileNotFoundError as e:
        # Critical lookup files missing - fail immediately
        logger.error(f"CRITICAL ERROR processing {zip_path}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Error processing {zip_path}: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_all_accessions_separations():
    """Process all accessions/separations ZIP files."""
    # Create output directories
    Path(EXTRACTED_DIR).mkdir(parents=True, exist_ok=True)
    Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create duplicate logger for separations/accessions (saves to separations_accessions directory)
    dup_logger = DuplicateLogger(log_dir="separations_accessions")
    
    # Find all ZIP files in the changes directory
    zip_files = glob.glob(os.path.join(CHANGES_DIR, "*.zip"))
    
    if not zip_files:
        logger.error(f"No ZIP files found in {CHANGES_DIR}")
        return []
    
    logger.info(f"Found {len(zip_files)} ZIP files to process")
    
    results = []
    
    for i, zip_path in enumerate(zip_files, 1):
        logger.info(f"\n[{i}/{len(zip_files)}] Processing {os.path.basename(zip_path)}...")
        try:
            result = process_accessions_separations_zip(zip_path, dup_logger)
            if result:
                results.append(result)
        except FileNotFoundError as e:
            logger.error(f"CRITICAL ERROR: Processing stopped due to missing lookup files: {e}")
            raise e
    
    # Save duplicate logs
    dup_logger.save_logs()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("PROCESSING COMPLETE")
    logger.info("="*60)
    logger.info(f"Successfully processed: {len(results)}/{len(zip_files)} files")
    
    if results:
        total_records = sum(r['records'] for r in results)
        total_size = sum(r['size_mb'] for r in results)
        logger.info(f"Total records: {total_records:,}")
        logger.info(f"Total size: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
        
        # Group by data type
        by_type = {}
        for result in results:
            data_type = result['data_type']
            if data_type not in by_type:
                by_type[data_type] = []
            by_type[data_type].append(result)
        
        for data_type, type_results in by_type.items():
            type_records = sum(r['records'] for r in type_results)
            type_size = sum(r['size_mb'] for r in type_results)
            logger.info(f"{data_type.title()}: {len(type_results)} files, {type_records:,} records, {type_size:.1f} MB")
    
    return results

if __name__ == "__main__":
    import sys
    
    # Check if changes directory exists
    if not os.path.exists(CHANGES_DIR):
        logger.error(f"Changes directory not found: {CHANGES_DIR}")
        sys.exit(1)
    
    # Clean up old parquet files first
    logger.info("Removing old accessions/separations parquet files...")
    for file in glob.glob(os.path.join(PARQUET_DIR, "fedscope_*accessions*.parquet")):
        os.remove(file)
        logger.info(f"  Removed {os.path.basename(file)}")
    for file in glob.glob(os.path.join(PARQUET_DIR, "fedscope_*separations*.parquet")):
        os.remove(file)
        logger.info(f"  Removed {os.path.basename(file)}")
    
    results = process_all_accessions_separations()
    
    if results:
        logger.info("\nGenerated files:")
        for result in results:
            logger.info(f"  {result['filename']}")
    else:
        logger.error("No files were processed successfully")
        sys.exit(1)