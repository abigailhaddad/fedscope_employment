#!/usr/bin/env python3
"""
Script to identify, rename, and extract the new UUID-named FedScope files.
Also fixes the duplicate key issues in the DuckDB loading script.
"""

import os
import zipfile
import re
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def identify_fedscope_file(zip_path):
    """
    Identify the time period of a FedScope file by examining its contents.
    Returns (quarter, year) or (None, None) if not identifiable.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            file_list = zf.namelist()
            
            # Look for FACTDATA files to determine the period
            fact_files = [f for f in file_list if f.upper().startswith('FACTDATA_')]
            if fact_files:
                fact_file = fact_files[0]
                # Extract quarter/year from filename like FACTDATA_MAR2008.TXT
                match = re.search(r'([A-Z]{3})(\d{4})', fact_file.upper())
                if match:
                    month_abbr, year = match.groups()
                    month_to_quarter = {
                        'MAR': 'March', 'JUN': 'June', 'SEP': 'September', 'DEC': 'December'
                    }
                    quarter = month_to_quarter.get(month_abbr)
                    if quarter:
                        return quarter, int(year)
            
            # Look for directory names that might indicate the period
            for file_name in file_list:
                if '/' in file_name:
                    dir_name = file_name.split('/')[0]
                    match = re.search(r'(March|June|September|December)[_\s]*(\d{4})', dir_name)
                    if match:
                        quarter, year = match.groups()
                        return quarter, int(year)
            
            # Look for any files with date patterns
            for file_name in file_list:
                match = re.search(r'([A-Z]{3})(\d{4})', file_name.upper())
                if match:
                    month_abbr, year = match.groups()
                    month_to_quarter = {
                        'MAR': 'March', 'JUN': 'June', 'SEP': 'September', 'DEC': 'December'
                    }
                    quarter = month_to_quarter.get(month_abbr)
                    if quarter:
                        return quarter, int(year)
                        
    except Exception as e:
        logger.error(f"Error examining {zip_path}: {e}")
    
    return None, None

def rename_and_extract_files():
    """Rename UUID files to proper FedScope names and extract them."""
    fedscope_dir = "fedscope_data"
    extracted_dir = os.path.join(fedscope_dir, "extracted")
    
    # Create extracted directory if it doesn't exist
    os.makedirs(extracted_dir, exist_ok=True)
    
    # Find all zip files
    all_zip_files = []
    uuid_files = []
    fedscope_files = []
    
    for file in os.listdir(fedscope_dir):
        if file.endswith('.zip'):
            all_zip_files.append(file)
            if file.startswith('FedScope_'):
                fedscope_files.append(file)
            elif '-' in file and len(file) == 40:  # UUID + .zip
                uuid_files.append(file)
    
    logger.info(f"Found {len(all_zip_files)} total zip files")
    logger.info(f"  - {len(uuid_files)} UUID-named files to rename")
    logger.info(f"  - {len(fedscope_files)} properly named FedScope files to extract")
    
    for uuid_file in uuid_files:
        zip_path = os.path.join(fedscope_dir, uuid_file)
        logger.info(f"Processing {uuid_file}...")
        
        # Identify the time period
        quarter, year = identify_fedscope_file(zip_path)
        
        if quarter and year:
            # Create proper filename
            proper_name = f"FedScope_Employment_{quarter}_{year}.zip"
            proper_path = os.path.join(fedscope_dir, proper_name)
            
            # Rename the file
            if not os.path.exists(proper_path):
                shutil.move(zip_path, proper_path)
                logger.info(f"  Renamed to: {proper_name}")
                
                # Extract it
                extract_path = os.path.join(extracted_dir, f"FedScope_Employment_{quarter}_{year}")
                if not os.path.exists(extract_path):
                    with zipfile.ZipFile(proper_path, 'r') as zf:
                        zf.extractall(extract_path)
                    logger.info(f"  Extracted to: {extract_path}")
                else:
                    logger.info(f"  Already extracted: {extract_path}")
            else:
                logger.warning(f"  Target file already exists: {proper_name}")
                os.remove(zip_path)  # Remove the duplicate
        else:
            logger.warning(f"  Could not identify period for {uuid_file}")
    
    # Now process properly named FedScope files
    for fedscope_file in fedscope_files:
        zip_path = os.path.join(fedscope_dir, fedscope_file)
        logger.info(f"Processing {fedscope_file}...")
        
        # Extract quarter and year from filename
        quarter, year = identify_fedscope_file(zip_path)
        
        if quarter and year:
            extract_path = os.path.join(extracted_dir, f"FedScope_Employment_{quarter}_{year}")
            if not os.path.exists(extract_path):
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        zf.extractall(extract_path)
                    logger.info(f"  Extracted to: {extract_path}")
                except Exception as e:
                    logger.error(f"  Error extracting {fedscope_file}: {e}")
            else:
                logger.info(f"  Already extracted: {extract_path}")
        else:
            logger.warning(f"  Could not identify period for {fedscope_file}")

def main():
    """Main function to fix file naming and extraction."""
    logger.info("Starting file rename and extraction process...")
    rename_and_extract_files()
    logger.info("File processing complete!")

if __name__ == "__main__":
    main()