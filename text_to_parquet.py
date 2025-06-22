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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXTRACTED_DIR = "fedscope_data/extracted"
PARQUET_DIR = "fedscope_data/parquet"

def get_quarter_year_from_filename(filename):
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

def load_lookup_tables(data_dir, dataset_key):
    """Load lookup tables for a dataset - copied from load_to_duckdb_robust.py"""
    logger.info(f"  Loading lookup tables for {dataset_key}...")
    
    # Lookup table mapping - exact same as DuckDB version
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
                
                # Handle duplicates by keeping first occurrence (same as DuckDB DISTINCT ON)
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
                        df = df.drop_duplicates(subset=['dataset_key', join_key], keep='first')
                
                lookups[table_name] = df
                logger.info(f"    Loaded {len(df)} rows into {table_name}")
                
            except Exception as e:
                logger.error(f"    Error loading {table_name}: {e}")
        else:
            logger.warning(f"  Lookup file not found: {file_path}")
    
    return lookups

def create_denormalized_records(fact_df, lookups, dataset_key):
    """Create denormalized records - exact same logic as DuckDB version"""
    
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

def process_single_dataset(dataset_dir):
    """Process a single dataset directory to create a Parquet file."""
    try:
        month, year = get_quarter_year_from_filename(dataset_dir)
        if not month or not year:
            logger.warning(f"Could not determine period for {dataset_dir}, skipping")
            return None
        
        dataset_key = f"{year}_{month}"
        dataset_path = os.path.join(EXTRACTED_DIR, dataset_dir)
        
        logger.info(f"Processing {dataset_key}...")
        
        # Find the data directory containing files
        data_dirs = []
        for root, dirs, files in os.walk(dataset_path):
            if any(f.upper().startswith('FACTDATA') for f in files):
                data_dirs.append(root)
        
        if not data_dirs:
            logger.warning(f"  No FACTDATA files found in {dataset_path}")
            return None
        
        data_dir = data_dirs[0]
        
        # Find fact file
        fact_files = glob.glob(os.path.join(data_dir, "FACTDATA*.TXT"))
        if not fact_files:
            logger.warning(f"  No fact data files found")
            return None
        
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
        lookups = load_lookup_tables(data_dir, dataset_key)
        
        # Create denormalized records
        logger.info(f"  Creating denormalized records...")
        denormalized_df = create_denormalized_records(fact_df, lookups, dataset_key)
        
        # Create output filename
        output_filename = f"fedscope_employment_{month}_{year}.parquet"
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
            'size_mb': size_mb
        }
        
    except Exception as e:
        logger.error(f"Error processing {dataset_dir}: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_all_datasets():
    """Process all datasets to create Parquet files."""
    # Create output directory
    Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    
    # Get all dataset directories
    dataset_dirs = sorted([d for d in os.listdir(EXTRACTED_DIR) 
                          if os.path.isdir(os.path.join(EXTRACTED_DIR, d))])
    
    logger.info(f"Found {len(dataset_dirs)} datasets to process")
    
    results = []
    
    # Sequential processing (keep it simple)
    for i, dataset_dir in enumerate(dataset_dirs, 1):
        logger.info(f"\n[{i}/{len(dataset_dirs)}] Processing {dataset_dir}...")
        result = process_single_dataset(dataset_dir)
        if result:
            results.append(result)
    
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