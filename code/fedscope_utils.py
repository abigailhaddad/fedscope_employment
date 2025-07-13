#!/usr/bin/env python3
"""
Shared utilities for FedScope data processing pipelines.

This module contains common functions used by both employment cube and 
separations/accessions processing pipelines.
"""

import pandas as pd
import os
import re
import json
import zipfile
import argparse
import sys
import shutil
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class DuplicateLogger:
    """Handles logging of lookup table duplicates during parquet creation"""
    
    def __init__(self, log_dir: str = "."):
        self.log_dir = Path(log_dir)
        self.json_file = self.log_dir / "lookup_duplicates_log.json"
        self.summary_file = self.log_dir / "lookup_duplicates_summary.txt"
        self.duplicates_data = {}
        
    def log_duplicates(self, table_name: str, dataset_key: str, df: pd.DataFrame, join_key: str):
        """
        Log duplicates found in a lookup table before dropping them.
        
        Args:
            table_name: Name of the lookup table (e.g., 'agency', 'occupation')
            dataset_key: Dataset identifier (e.g., '2023_September')
            df: DataFrame with duplicates
            join_key: Column name used for joining (e.g., 'agysub', 'occ')
        """
        # Find duplicates
        duplicated_mask = df.duplicated(subset=['dataset_key', join_key], keep=False)
        if not duplicated_mask.any():
            return  # No duplicates found
            
        duplicated_df = df[duplicated_mask].copy()
        
        # Group by join key to process each duplicate set
        for join_value, group in duplicated_df.groupby(join_key):
            if len(group) <= 1:
                continue  # Not actually duplicated
                
            # First record is kept, rest are discarded
            kept_record = group.iloc[0].to_dict()
            discarded_records = [row.to_dict() for _, row in group.iloc[1:].iterrows()]
            
            # Create duplicate entry
            duplicate_entry = {
                'dataset_key': dataset_key,
                'code': join_value,
                'count': len(group),
                'kept': kept_record,
                'discarded': discarded_records
            }
            
            # Add to duplicates data
            if table_name not in self.duplicates_data:
                self.duplicates_data[table_name] = []
            self.duplicates_data[table_name].append(duplicate_entry)
            
            logger.debug(f"Logged {len(group)} duplicates for {table_name}.{join_key}='{join_value}' in {dataset_key}")
    
    def save_logs(self):
        """Save duplicate logs to JSON and summary files"""
        if not self.duplicates_data:
            logger.info("No duplicates found to log")
            # Create empty files to indicate no duplicates were found
            with open(self.json_file, 'w') as f:
                json.dump({}, f, indent=2)
            
            with open(self.summary_file, 'w') as f:
                f.write("FedScope Lookup Table Duplicates Summary\n")
                f.write("=" * 50 + "\n\n")
                f.write("NO DUPLICATES FOUND\n\n")
                f.write("All lookup tables in this dataset are unique - no duplicate codes were detected.\n")
                f.write(f"This means each lookup code maps to exactly one description.\n\n")
                f.write(f"Log generated at: {self.log_dir}\n")
            
            logger.info(f"Created empty duplicate log files at {self.log_dir}")
            return
            
        # Load existing data if files exist
        existing_json_data = {}
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r') as f:
                    existing_json_data = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load existing JSON log: {e}")
        
        # Merge new data with existing
        for table_name, duplicates in self.duplicates_data.items():
            if table_name not in existing_json_data:
                existing_json_data[table_name] = []
            
            # Remove duplicates from existing data for this dataset
            dataset_keys = {dup['dataset_key'] for dup in duplicates}
            existing_json_data[table_name] = [
                dup for dup in existing_json_data[table_name] 
                if dup['dataset_key'] not in dataset_keys
            ]
            
            # Add new duplicates
            existing_json_data[table_name].extend(duplicates)
        
        # Save JSON file
        with open(self.json_file, 'w') as f:
            json.dump(existing_json_data, f, indent=2)
        
        # Create summary file
        self._create_summary(existing_json_data)
        
        total_duplicates = sum(len(table_dups) for table_dups in existing_json_data.values())
        logger.info(f"Saved duplicate logs: {total_duplicates} duplicate entries across {len(existing_json_data)} tables")
    
    def _create_summary(self, all_data: Dict):
        """Create human-readable summary file"""
        with open(self.summary_file, 'w') as f:
            f.write("FedScope Lookup Table Duplicates Summary\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("This file documents cases where lookup tables contain multiple records\n")
            f.write("for the same code within a dataset. When joining, we use the first\n")
            f.write("occurrence (by ROWID) and discard the others.\n\n")
            
            # Calculate summary stats
            total_duplicates = sum(len(table_dups) for table_dups in all_data.values())
            total_tables = len([t for t in all_data.values() if t])
            
            f.write(f"SUMMARY: Found {total_duplicates} duplicate codes across {total_tables} tables\n")
            f.write("Most duplicates are from early years (1998-2003) when agencies were renamed.\n\n\n")
            
            # Write details for each table
            for table_name, duplicates in all_data.items():
                if not duplicates:
                    continue
                    
                f.write("=" * 20 + f" {table_name.upper()} TABLE " + "=" * 20 + "\n")
                f.write(f"Found {len(duplicates)} duplicate codes in this table\n\n")
                
                # Sort by dataset_key, then by code
                sorted_duplicates = sorted(duplicates, key=lambda x: (x['dataset_key'], x['code']))
                
                for dup in sorted_duplicates:
                    f.write(f"Dataset: {dup['dataset_key']} | Code: '{dup['code']}' | {dup['count']} duplicates\n")
                    f.write("-" * 60 + "\n")
                    
                    # Show kept record
                    kept = dup['kept']
                    f.write("✅ KEPT (first occurrence):\n")
                    if table_name == 'agency':
                        f.write(f"   Agency: {kept.get('agyt', 'N/A')}\n")
                        f.write(f"   Sub-Agency: {kept.get('agysubt', 'N/A')}\n")
                    elif table_name == 'occupation':
                        f.write(f"   Occupation: {kept.get('occt', 'N/A')}\n")
                        f.write(f"   Family: {kept.get('occfamt', 'N/A')}\n")
                    else:
                        # Generic display for other tables
                        desc_fields = [k for k in kept.keys() if k.endswith('t') and k != 'dataset_key']
                        if desc_fields:
                            f.write(f"   Description: {kept.get(desc_fields[0], 'N/A')}\n")
                    
                    # Show discarded records
                    f.write("\n❌ DISCARDED:\n")
                    for i, disc in enumerate(dup['discarded'], 1):
                        f.write(f"   #{i}: ")
                        if table_name == 'agency':
                            f.write(f"Agency: {disc.get('agyt', 'N/A')}\n")
                            f.write(f"       Sub-Agency: {disc.get('agysubt', 'N/A')}\n")
                        elif table_name == 'occupation':
                            f.write(f"Occupation: {disc.get('occt', 'N/A')}\n")
                            f.write(f"       Family: {disc.get('occfamt', 'N/A')}\n")
                        else:
                            desc_fields = [k for k in disc.keys() if k.endswith('t') and k != 'dataset_key']
                            if desc_fields:
                                f.write(f"Description: {disc.get(desc_fields[0], 'N/A')}\n")
                    
                    f.write("\n")
                
                f.write("\n")


def handle_lookup_duplicates(table_name: str, dataset_key: str, df: pd.DataFrame, 
                           join_key: str, logger_instance: Optional[DuplicateLogger] = None) -> pd.DataFrame:
    """
    Handle duplicates in a lookup table by logging them and keeping the first occurrence.
    
    Args:
        table_name: Name of the lookup table
        dataset_key: Dataset identifier
        df: DataFrame with potential duplicates
        join_key: Column name used for joining
        logger_instance: DuplicateLogger instance (optional)
    
    Returns:
        DataFrame with duplicates removed
    """
    if logger_instance:
        logger_instance.log_duplicates(table_name, dataset_key, df, join_key)
    
    # Remove duplicates, keeping first occurrence
    return df.drop_duplicates(subset=['dataset_key', join_key], keep='first')


def get_quarter_year_from_filename(filename: str) -> Tuple[Optional[str], Optional[int]]:
    """Extract month and year from FedScope filename patterns."""
    # Handle patterns like FACTDATA_DEC2018.TXT or directory names like FedScope_Employment_December_2018
    
    # First try the file pattern
    match = re.search(r'([A-Z]{3})(\d{4})', filename.upper())
    if match:
        month_abbr, year = match.groups()
        month_mapping = {
            'MAR': 'March', 'JUN': 'June', 'SEP': 'September', 'DEC': 'December'
        }
        month = month_mapping.get(month_abbr, 'UNKNOWN')
        return month, int(year)
    
    # Then try the directory pattern
    match = re.search(r'(March|June|September|December)_(\d{4})', filename)
    if match:
        month_name, year = match.groups()
        return month_name, int(year)
    
    return None, None


def load_lookup_tables(data_dir: str, dataset_key: str, duplicate_logger: Optional[DuplicateLogger] = None) -> Dict[str, pd.DataFrame]:
    """Load lookup tables for a dataset"""
    logger.info(f"  Loading lookup tables for {dataset_key}...")
    
    # Determine if this is separations/accessions data or employment cube data
    is_sep_acc_data = 'separations' in dataset_key.lower() or 'accessions' in dataset_key.lower()
    
    # Lookup table mapping - different files for separations/accessions vs employment
    if is_sep_acc_data:
        lookup_files = {
            'agelvl': 'DTagelvl.txt',
            'agency': 'DTagy.txt', 
            'date': 'DTefdate.txt',  # separations/accessions use efdate instead of date
            'education': 'DTedlvl.txt',
            'gsegrd': 'DTgsegrd.txt',
            'location': 'DTloc.txt',
            'loslvl': 'DTloslvl.txt',
            'occupation': 'DTocc.txt',
            'patco': 'DTpatco.txt',
            'ppgrd': 'DTppgrd.txt',
            'salary_level': 'DTsallvl.txt',
            'appointment': 'DTtoa.txt',
            'work_schedule': 'DTwrksch.txt',
            'work_status': 'DTwkstat.txt'
            # Note: separations/accessions don't have DTpp.txt, DTstemocc.txt, DTsuper.txt
        }
    else:
        lookup_files = {
            'agelvl': 'DTagelvl.txt',
            'agency': 'DTagy.txt', 
            'date': 'DTdate.txt',
            'education': 'DTedlvl.txt',
            'gsegrd': 'DTgsegrd.txt',
            'location': 'DTloc.txt',
            'loslvl': 'DTloslvl.txt',
            'occupation': 'DTocc.txt',
            'patco': 'DTpatco.txt',
            'payplan': 'DTpp.txt',
            'ppgrd': 'DTppgrd.txt',
            'salary_level': 'DTsallvl.txt',
            'stemocc': 'DTstemocc.txt',
            'supervisory': 'DTsuper.txt',
            'appointment': 'DTtoa.txt',
            'work_schedule': 'DTwrksch.txt',
            'work_status': 'DTwkstat.txt'
        }
    
    lookups = {}
    
    for table_name, file_pattern in lookup_files.items():
        file_path = os.path.join(data_dir, file_pattern)
        if os.path.exists(file_path):
            try:
                # Read lookup table
                df = pd.read_csv(file_path, sep=',', encoding='latin-1', dtype=str)
                
                # Clean column names
                df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
                
                # Add dataset key
                df.insert(0, 'dataset_key', dataset_key)
                
                # Map each lookup table to its correct join key column
                join_key_mapping = {
                    'agelvl': 'agelvl',
                    'agency': 'agysub', 
                    'date': 'datecode',
                    'education': 'edlvl',
                    'gsegrd': 'gsegrd',
                    'location': 'loc',
                    'loslvl': 'loslvl',
                    'occupation': 'occ',
                    'patco': 'patco',
                    'payplan': 'pp',
                    'ppgrd': 'ppgrd',
                    'salary_level': 'sallvl',
                    'stemocc': 'stemocc',
                    'supervisory': 'supervis',
                    'appointment': 'toa',
                    'work_schedule': 'worksch',
                    'work_status': 'workstat'
                }
                
                if table_name in join_key_mapping:
                    join_key = join_key_mapping[table_name]
                    if join_key in df.columns:
                        df = handle_lookup_duplicates(table_name, dataset_key, df, join_key, duplicate_logger)
                
                lookups[table_name] = df
                logger.info(f"    Loaded {len(df)} rows into {table_name}")
                
            except Exception as e:
                logger.error(f"    Error loading {table_name}: {e}")
        else:
            # For separations/accessions data, missing lookup files are expected for some tables
            if is_sep_acc_data and table_name in ['payplan', 'stemocc', 'supervisory']:
                logger.debug(f"  Skipping {table_name} (not available for separations/accessions)")
            # For employment cube data, some lookup files didn't exist in older datasets
            elif not is_sep_acc_data and table_name in ['payplan', 'stemocc']:
                logger.debug(f"  Skipping {table_name} (not available in this dataset)")
            else:
                logger.warning(f"  Lookup file not found: {file_pattern} (skipping)")
    
    return lookups


def create_denormalized_records(fact_df: pd.DataFrame, lookups: Dict[str, pd.DataFrame], dataset_key: str) -> pd.DataFrame:
    """Create denormalized records by joining fact data with lookup tables"""
    
    # Start with fact data
    result = fact_df.copy()
    
    # Handle pay plan field
    pp_field = None
    if "pp" in result.columns:
        pp_field = result['pp']
    
    # Clean all string columns that will be used for joins
    for col in result.columns:
        if result[col].dtype == 'object':
            result[col] = result[col].astype(str).str.strip()
    
    # Now do the exact same joins as DuckDB version
    # LEFT JOIN agelvl
    if 'agelvl' in lookups and 'agelvl' in result.columns:
        a = lookups['agelvl'].copy()
        a['agelvl'] = a['agelvl'].astype(str).str.strip()
        a = a[['dataset_key', 'agelvl', 'agelvlt']].drop_duplicates(['dataset_key', 'agelvl'])
        result = result.merge(a, on=['dataset_key', 'agelvl'], how='left')
    
    # LEFT JOIN education  
    if 'education' in lookups and 'edlvl' in result.columns:
        e = lookups['education'].copy()
        e['edlvl'] = e['edlvl'].astype(str).str.strip()
        e = e[['dataset_key', 'edlvl', 'edlvlt']].drop_duplicates(['dataset_key', 'edlvl'])
        result = result.merge(e, on=['dataset_key', 'edlvl'], how='left')
    
    # LEFT JOIN gsegrd
    if 'gsegrd' in lookups and 'gsegrd' in result.columns:
        # DuckDB version: g.gsegrd as gsegrdt - the lookup value becomes gsegrdt
        g = lookups['gsegrd'].copy()
        g['gsegrd'] = g['gsegrd'].astype(str).str.strip()
        # The lookup table's gsegrd column becomes gsegrdt in output
        if 'gsegrd' in g.columns and len(g.columns) >= 3:  # dataset_key, code, description
            # Rename the description column to gsegrdt
            cols = list(g.columns)
            desc_col_idx = 2  # Usually the third column is the description
            cols[desc_col_idx] = 'gsegrdt'
            g.columns = cols
        g = g.drop_duplicates(['dataset_key', 'gsegrd'])
        result = result.merge(g, on=['dataset_key', 'gsegrd'], how='left')
    
    # LEFT JOIN loslvl
    if 'loslvl' in lookups and 'loslvl' in result.columns:
        los = lookups['loslvl'][['dataset_key', 'loslvl', 'loslvlt']].drop_duplicates(['dataset_key', 'loslvl'])
        result = result.merge(los, on=['dataset_key', 'loslvl'], how='left')
    
    # LEFT JOIN occupation
    if 'occupation' in lookups and 'occ' in result.columns:
        # The DuckDB version expects: o.occ as occfam, o.occt, o.occfamt
        # This means the lookup table has columns: occ (code), occ (family), occt (title), occfamt (family title)
        o = lookups['occupation'].copy()
        o['occ'] = o['occ'].astype(str).str.strip()
        
        # Handle the duplicate 'occ' column issue - rename second occurrence to 'occfam'
        cols = list(o.columns)
        if cols.count('occ') > 1:
            # Find second occurrence and rename to occfam
            first_idx = cols.index('occ')
            second_idx = cols.index('occ', first_idx + 1)
            cols[second_idx] = 'occfam'
            o.columns = cols
        
        o = o.drop_duplicates(['dataset_key', 'occ'])
        result = result.merge(o, on=['dataset_key', 'occ'], how='left')
    
    # LEFT JOIN patco
    if 'patco' in lookups and 'patco' in result.columns:
        p = lookups['patco'].copy()
        p['patco'] = p['patco'].astype(str).str.strip()
        p = p[['dataset_key', 'patco', 'patcot']].drop_duplicates(['dataset_key', 'patco'])
        result = result.merge(p, on=['dataset_key', 'patco'], how='left')
    
    # LEFT JOIN ppgrd
    if 'ppgrd' in lookups and 'ppgrd' in result.columns:
        # DuckDB version: pp.ppgrd as ppgrdt
        pp = lookups['ppgrd'].copy()
        pp['ppgrd'] = pp['ppgrd'].astype(str).str.strip()
        # The lookup table's ppgrd description becomes ppgrdt in output
        if 'ppgrd' in pp.columns and len(pp.columns) >= 3:
            cols = list(pp.columns)
            desc_col_idx = 2  # Usually the third column is the description
            cols[desc_col_idx] = 'ppgrdt'
            pp.columns = cols
        pp = pp.drop_duplicates(['dataset_key', 'ppgrd'])
        result = result.merge(pp, on=['dataset_key', 'ppgrd'], how='left')
    
    # LEFT JOIN salary_level
    if 'salary_level' in lookups and 'sallvl' in result.columns:
        sl = lookups['salary_level'][['dataset_key', 'sallvl', 'sallvlt']].drop_duplicates(['dataset_key', 'sallvl'])
        result = result.merge(sl, on=['dataset_key', 'sallvl'], how='left')
    
    # LEFT JOIN stemocc
    if 'stemocc' in lookups and 'stemocc' in result.columns:
        st = lookups['stemocc'][['dataset_key', 'stemocc', 'stemocct']].drop_duplicates(['dataset_key', 'stemocc'])
        result = result.merge(st, on=['dataset_key', 'stemocc'], how='left')
    
    # LEFT JOIN supervisory
    if 'supervisory' in lookups and 'supervis' in result.columns:
        s = lookups['supervisory'][['dataset_key', 'supervis', 'supervist']].drop_duplicates(['dataset_key', 'supervis'])
        result = result.merge(s, on=['dataset_key', 'supervis'], how='left')
    
    # LEFT JOIN appointment
    if 'appointment' in lookups and 'toa' in result.columns:
        t = lookups['appointment'][['dataset_key', 'toa', 'toat']].drop_duplicates(['dataset_key', 'toa'])
        result = result.merge(t, on=['dataset_key', 'toa'], how='left')
    
    # LEFT JOIN work_schedule
    if 'work_schedule' in lookups and 'worksch' in result.columns:
        # DuckDB version: ws.workscht as wrkscht
        ws = lookups['work_schedule'].copy()
        ws['worksch'] = ws['worksch'].astype(str).str.strip()
        # Rename the description column to wrkscht if it exists
        if 'workscht' in ws.columns:
            ws = ws.rename(columns={'workscht': 'wrkscht'})
        ws = ws.drop_duplicates(['dataset_key', 'worksch'])
        result = result.merge(ws, on=['dataset_key', 'worksch'], how='left')
    
    # LEFT JOIN work_status  
    if 'work_status' in lookups and 'workstat' in result.columns:
        # DuckDB version: wst.workstatt as wkstatt
        wst = lookups['work_status'].copy()
        wst['workstat'] = wst['workstat'].astype(str).str.strip()
        # Rename the description column to wkstatt if it exists
        if 'workstatt' in wst.columns:
            wst = wst.rename(columns={'workstatt': 'wkstatt'})
        wst = wst.drop_duplicates(['dataset_key', 'workstat'])
        result = result.merge(wst, on=['dataset_key', 'workstat'], how='left')
    
    # LEFT JOIN agency
    if 'agency' in lookups and 'agysub' in result.columns:
        ag = lookups['agency'].copy()
        ag['agysub'] = ag['agysub'].astype(str).str.strip()
        ag = ag[['dataset_key', 'agysub', 'agy', 'agysubt']].drop_duplicates(['dataset_key', 'agysub'])
        result = result.merge(ag, on=['dataset_key', 'agysub'], how='left')
    
    # LEFT JOIN location
    if 'location' in lookups and 'loc' in result.columns:
        # Normalize location codes - pad with leading zeros to match lookup table format
        result['loc'] = result['loc'].astype(str).str.strip().str.zfill(2)
        
        # Also normalize lookup table location codes
        loc_lookup = lookups['location'].copy()
        loc_lookup['loc'] = loc_lookup['loc'].astype(str).str.strip().str.zfill(2)
        
        l = loc_lookup[['dataset_key', 'loc', 'loct']].drop_duplicates(['dataset_key', 'loc'])
        result = result.merge(l, on=['dataset_key', 'loc'], how='left')
    
    # LEFT JOIN payplan (conditional like DuckDB version)
    if pp_field is not None and 'payplan' in lookups:
        pl = lookups['payplan'][['dataset_key', 'pp', 'ppt']].drop_duplicates(['dataset_key', 'pp'])
        result = result.merge(pl, on=['dataset_key', 'pp'], how='left')
    
    # Handle worksch -> wrksch field aliasing
    if 'worksch' in result.columns and 'wrksch' not in result.columns:
        result['wrksch'] = result['worksch']
    if 'workstat' in result.columns and 'wkstat' not in result.columns:
        result['wkstat'] = result['workstat']
    
    return result


# Pipeline utility functions

def setup_code_path():
    """Add code directory to Python path for imports"""
    code_dir = Path("code")
    if str(code_dir) not in sys.path:
        sys.path.insert(0, str(code_dir))

def extract_zip_files(raw_dir: str, extracted_dir: str) -> int:
    """
    Extract ZIP files from raw directory to extracted directory.
    
    Args:
        raw_dir: Directory containing ZIP files
        extracted_dir: Directory to extract files to
    
    Returns:
        Number of files extracted
    """
    raw_path = Path(raw_dir)
    extracted_path = Path(extracted_dir)
    
    # Create extracted directory if it doesn't exist
    extracted_path.mkdir(exist_ok=True)
    
    # Find all zip files
    zip_files = list(raw_path.glob("*.zip"))
    logger.info(f"Found {len(zip_files)} zip files to extract")
    
    extracted_count = 0
    for zip_file in zip_files:
        # Determine output directory name
        output_name = zip_file.stem  # Remove .zip extension
        output_dir = extracted_path / output_name
        
        # Skip if already extracted
        if output_dir.exists():
            logger.info(f"Already extracted: {output_name}")
            continue
            
        logger.info(f"Extracting: {zip_file.name}")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                zf.extractall(output_dir)
            extracted_count += 1
        except Exception as e:
            logger.error(f"Failed to extract {zip_file.name}: {e}")
    
    logger.info(f"Extraction complete: {extracted_count} new files extracted")
    return extracted_count

def validate_parquet_files(parquet_dir: str, required_cols: Optional[List[str]] = None) -> Tuple[int, int, int]:
    """
    Validate parquet files in a directory.
    
    Args:
        parquet_dir: Directory containing parquet files
        required_cols: List of required columns (optional)
    
    Returns:
        Tuple of (valid_count, total_files, total_records)
    """
    parquet_path = Path(parquet_dir)
    if not parquet_path.exists():
        logger.error("Parquet directory does not exist")
        return 0, 0, 0
    
    parquet_files = list(parquet_path.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files to validate")
    
    valid_count = 0
    total_records = 0
    
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            record_count = len(df)
            total_records += record_count
            
            # Check for required columns if specified
            if required_cols:
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.warning(f"✓ {parquet_file.name}: {record_count:,} records (missing columns: {missing_cols})")
                else:
                    logger.info(f"✓ {parquet_file.name}: {record_count:,} records")
            else:
                logger.info(f"✓ {parquet_file.name}: {record_count:,} records")
            
            valid_count += 1
        except Exception as e:
            logger.error(f"✗ {parquet_file.name}: {e}")
    
    logger.info(f"Validation complete: {valid_count}/{len(parquet_files)} files valid")
    if total_records > 0:
        logger.info(f"Total records across all files: {total_records:,}")
    
    return valid_count, len(parquet_files), total_records

def create_pipeline_parser(description: str) -> argparse.ArgumentParser:
    """Create a standard argument parser for pipelines"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--extract', action='store_true', help='Extract zip files only')
    parser.add_argument('--parquet', action='store_true', help='Create parquet files only')
    parser.add_argument('--validate', action='store_true', help='Validate parquet files only')
    parser.add_argument('--docs', action='store_true', help='Collect documentation files')
    return parser

def setup_logging():
    """Setup standard logging configuration for pipelines"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def collect_documentation_files(source_dir: str, target_dir: str, pipeline_name: str) -> int:
    """
    Collect documentation files from a pipeline's extracted directory.
    
    Args:
        source_dir: Directory containing extracted data (e.g., 'employment_cube/extracted')
        target_dir: Target directory for documentation (e.g., 'employment_cube/documentation')
        pipeline_name: Name of pipeline for logging (e.g., 'Employment Cube')
    
    Returns:
        Number of files collected
    """
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    if not source_path.exists():
        logger.warning(f"Source directory does not exist: {source_dir}")
        return 0
    
    # Create target directory
    target_path.mkdir(parents=True, exist_ok=True)
    
    # File patterns to collect
    patterns = ['*.pdf', '*.PDF']
    
    collected_count = 0
    
    # Search recursively through all subdirectories
    for pattern in patterns:
        files = list(source_path.rglob(pattern))
        
        for file_path in files:
            # Skip files that are clearly data files, not documentation
            if any(skip in file_path.name.upper() for skip in ['FACTDATA', 'SEPDATA', 'ACCDATA']):
                continue
                
            # Create meaningful filename with source info
            relative_path = file_path.relative_to(source_path)
            # Replace path separators with underscores to flatten structure
            new_name = str(relative_path).replace(os.sep, '_')
            target_file = target_path / new_name
            
            # Skip if file already exists and is the same size
            if target_file.exists() and target_file.stat().st_size == file_path.stat().st_size:
                logger.debug(f"Skipping existing file: {new_name}")
                continue
            
            try:
                shutil.copy2(file_path, target_file)
                logger.info(f"Collected: {new_name}")
                collected_count += 1
            except Exception as e:
                logger.error(f"Failed to copy {file_path}: {e}")
    
    if collected_count > 0:
        logger.info(f"{pipeline_name}: Collected {collected_count} documentation files to {target_dir}")
    
    return collected_count

def collect_all_documentation():
    """Collect documentation from both pipelines."""
    logger.info("=== COLLECTING DOCUMENTATION FILES ===")
    
    total_collected = 0
    
    # Employment cube documentation
    emp_count = collect_documentation_files(
        source_dir="employment_cube/extracted",
        target_dir="employment_cube/documentation", 
        pipeline_name="Employment Cube"
    )
    total_collected += emp_count
    
    # Separations/accessions documentation
    sep_count = collect_documentation_files(
        source_dir="separations_accessions/extracted",
        target_dir="separations_accessions/documentation",
        pipeline_name="Separations & Accessions"
    )
    total_collected += sep_count
    
    logger.info(f"=== DOCUMENTATION COLLECTION COMPLETE ===")
    logger.info(f"Total files collected: {total_collected}")
    
    return total_collected