#!/usr/bin/env python3
"""
Direct FedScope Employment data loader to PostgreSQL - Fixed Version.
Efficiently loads data directly to PostgreSQL without DuckDB intermediate step.
"""

import psycopg2
import pandas as pd
import os
import re
import glob
from pathlib import Path
import logging
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_quarter_year_from_filename(filename):
    """Extract quarter and year from FedScope filename patterns."""
    # Handle patterns like FACTDATA_DEC2018.TXT or directory names like FedScope_Employment_December_2018
    
    # First try the file pattern
    match = re.search(r'([A-Z]{3})(\d{4})', filename.upper())
    if match:
        month_abbr, year = match.groups()
        month_to_quarter = {
            'MAR': 'Q1', 'JUN': 'Q2', 'SEP': 'Q3', 'DEC': 'Q4'
        }
        quarter = month_to_quarter.get(month_abbr, 'UNKNOWN')
        return quarter, int(year)
    
    # Then try the directory pattern
    match = re.search(r'(March|June|September|December)_(\d{4})', filename)
    if match:
        month_name, year = match.groups()
        month_to_quarter = {
            'March': 'Q1', 'June': 'Q2', 'September': 'Q3', 'December': 'Q4'
        }
        quarter = month_to_quarter.get(month_name, 'UNKNOWN')
        return quarter, int(year)
    
    return None, None

def create_postgres_schema(pg_conn):
    """Create PostgreSQL schema for FedScope tables."""
    logger.info("Creating PostgreSQL schema...")
    
    cur = pg_conn.cursor()
    
    # Drop existing tables if they exist
    tables_to_drop = [
        'fedscope_employment_facts', 'fedscope_lookup_agelvl', 'fedscope_lookup_agency', 
        'fedscope_lookup_date', 'fedscope_lookup_education', 'fedscope_lookup_gsegrd', 
        'fedscope_lookup_location', 'fedscope_lookup_loslvl', 'fedscope_lookup_occupation',
        'fedscope_lookup_patco', 'fedscope_lookup_payplan', 'fedscope_lookup_ppgrd', 
        'fedscope_lookup_salary_level', 'fedscope_lookup_stemocc', 'fedscope_lookup_supervisory', 
        'fedscope_lookup_appointment', 'fedscope_lookup_work_schedule', 'fedscope_lookup_work_status'
    ]
    
    for table in tables_to_drop:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception as e:
            logger.warning(f"Could not drop {table}: {e}")
    
    # Create lookup tables with proper schemas
    
    # Age Level
    cur.execute("""
        CREATE TABLE fedscope_lookup_agelvl (
            dataset_key VARCHAR(20) NOT NULL,
            agelvl_code VARCHAR(10) NOT NULL,
            agelvl_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, agelvl_code)
        )
    """)
    
    # Agency (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_agency (
            dataset_key VARCHAR(20) NOT NULL,
            agytyp INTEGER,
            agytyp_desc TEXT,
            agy_code VARCHAR(10),
            agy_desc TEXT,
            agysub_code VARCHAR(10) NOT NULL,
            agysub_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, agysub_code)
        )
    """)
    
    # Date
    cur.execute("""
        CREATE TABLE fedscope_lookup_date (
            dataset_key VARCHAR(20) NOT NULL,
            datecode VARCHAR(10) NOT NULL,
            datecode_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, datecode)
        )
    """)
    
    # Education (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_education (
            dataset_key VARCHAR(20) NOT NULL,
            edlvltyp INTEGER,
            edlvltyp_desc TEXT,
            edlvl_code VARCHAR(10) NOT NULL,
            edlvl_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, edlvl_code)
        )
    """)
    
    # GS Grade
    cur.execute("""
        CREATE TABLE fedscope_lookup_gsegrd (
            dataset_key VARCHAR(20) NOT NULL,
            gsegrd_code VARCHAR(10) NOT NULL,
            gsegrd_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, gsegrd_code)
        )
    """)
    
    # Location (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_location (
            dataset_key VARCHAR(20) NOT NULL,
            loctyp INTEGER,
            loctyp_desc TEXT,
            loc_code VARCHAR(10) NOT NULL,
            loc_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, loc_code)
        )
    """)
    
    # Length of Service Level
    cur.execute("""
        CREATE TABLE fedscope_lookup_loslvl (
            dataset_key VARCHAR(20) NOT NULL,
            loslvl_code VARCHAR(10) NOT NULL,
            loslvl_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, loslvl_code)
        )
    """)
    
    # Occupation (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_occupation (
            dataset_key VARCHAR(20) NOT NULL,
            occtyp INTEGER,
            occtyp_desc TEXT,
            occfam VARCHAR(10),
            occfam_desc TEXT,
            occ_code VARCHAR(10) NOT NULL,
            occ_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, occ_code)
        )
    """)
    
    # PATCO
    cur.execute("""
        CREATE TABLE fedscope_lookup_patco (
            dataset_key VARCHAR(20) NOT NULL,
            patco_code VARCHAR(10) NOT NULL,
            patco_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, patco_code)
        )
    """)
    
    # Pay Plan
    cur.execute("""
        CREATE TABLE fedscope_lookup_payplan (
            dataset_key VARCHAR(20) NOT NULL,
            pp_code VARCHAR(10) NOT NULL,
            pp_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, pp_code)
        )
    """)
    
    # Pay Plan Grade (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_ppgrd (
            dataset_key VARCHAR(20) NOT NULL,
            pptyp INTEGER,
            pptyp_desc TEXT,
            ppgroup VARCHAR(10),
            ppgroup_desc TEXT,
            payplan VARCHAR(10),
            payplan_desc TEXT,
            ppgrd_code VARCHAR(10) NOT NULL,
            ppgrd_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, ppgrd_code)
        )
    """)
    
    # Salary Level
    cur.execute("""
        CREATE TABLE fedscope_lookup_salary_level (
            dataset_key VARCHAR(20) NOT NULL,
            sallvl_code VARCHAR(10) NOT NULL,
            sallvl_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, sallvl_code)
        )
    """)
    
    # STEM Occupation (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_stemocc (
            dataset_key VARCHAR(20) NOT NULL,
            stemagg VARCHAR(10),
            stemagg_desc TEXT,
            stemtyp VARCHAR(10),
            stemtyp_desc TEXT,
            stemocc_code VARCHAR(10) NOT NULL,
            stemocc_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, stemocc_code)
        )
    """)
    
    # Supervisory (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_supervisory (
            dataset_key VARCHAR(20) NOT NULL,
            supertyp INTEGER,
            supertyp_desc TEXT,
            supervis_code VARCHAR(10) NOT NULL,
            supervis_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, supervis_code)
        )
    """)
    
    # Type of Appointment (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_appointment (
            dataset_key VARCHAR(20) NOT NULL,
            toatyp INTEGER,
            toatyp_desc TEXT,
            toa_code VARCHAR(10) NOT NULL,
            toa_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, toa_code)
        )
    """)
    
    # Work Schedule (hierarchical)
    cur.execute("""
        CREATE TABLE fedscope_lookup_work_schedule (
            dataset_key VARCHAR(20) NOT NULL,
            wstyp INTEGER,
            wstyp_desc TEXT,
            worksch_code VARCHAR(10) NOT NULL,
            worksch_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, worksch_code)
        )
    """)
    
    # Work Status
    cur.execute("""
        CREATE TABLE fedscope_lookup_work_status (
            dataset_key VARCHAR(20) NOT NULL,
            workstat_code VARCHAR(10) NOT NULL,
            workstat_desc TEXT,
            raw_data TEXT,
            PRIMARY KEY (dataset_key, workstat_code)
        )
    """)
    
    # Employment Facts (main table)
    cur.execute("""
        CREATE TABLE fedscope_employment_facts (
            id BIGINT PRIMARY KEY,
            dataset_key VARCHAR(20) NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            year INTEGER NOT NULL,
            raw_data TEXT NOT NULL,
            employment INTEGER NOT NULL DEFAULT 0,
            salary DECIMAL(12,2),
            length_of_service DECIMAL(8,2),
            data_source VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    pg_conn.commit()
    cur.close()
    logger.info("PostgreSQL schema created successfully")

def create_indexes(pg_conn):
    """Create indexes for better performance."""
    logger.info("Creating indexes...")
    
    cur = pg_conn.cursor()
    
    indexes = [
        "CREATE INDEX idx_employment_facts_dataset_key ON fedscope_employment_facts(dataset_key)",
        "CREATE INDEX idx_employment_facts_year_quarter ON fedscope_employment_facts(year, quarter)",
        "CREATE INDEX idx_employment_facts_salary ON fedscope_employment_facts(salary)",
        "CREATE INDEX idx_agency_dataset_key ON fedscope_lookup_agency(dataset_key)",
        "CREATE INDEX idx_occupation_dataset_key ON fedscope_lookup_occupation(dataset_key)",
        "CREATE INDEX idx_location_dataset_key ON fedscope_lookup_location(dataset_key)",
        "CREATE INDEX idx_education_dataset_key ON fedscope_lookup_education(dataset_key)"
    ]
    
    for index_sql in indexes:
        try:
            cur.execute(index_sql)
        except Exception as e:
            logger.warning(f"Could not create index: {e}")
    
    pg_conn.commit()
    cur.close()
    logger.info("Indexes created successfully")

def load_lookup_file_to_postgres(pg_conn, file_path, dataset_key, batch_size=50000):
    """Load lookup file directly into appropriate PostgreSQL table."""
    try:
        df = pd.read_csv(file_path, dtype=str)
        filename = os.path.basename(file_path)
        table_name = filename.replace('DT', '').replace('.txt', '').lower()
        
        logger.info(f"Loading {filename}: {len(df)} rows, {len(df.columns)} columns")
        
        # Add dataset_key and raw_data to all records
        df['dataset_key'] = dataset_key
        df['raw_data'] = df.apply(lambda row: '|'.join([f"{col}:{val}" for col, val in row.items() if col not in ['dataset_key', 'raw_data']]), axis=1)
        
        records_loaded = 0
        
        try:
            cur = pg_conn.cursor()
            
            if table_name == 'agelvl' and 'AGELVL' in df.columns:
                df_clean = df[['dataset_key', 'AGELVL', 'AGELVLT', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'agelvl_code', 'agelvl_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_agelvl (dataset_key, agelvl_code, agelvl_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'agy' and 'AGYSUB' in df.columns:
                # Handle agency hierarchical structure
                cols = ['dataset_key', 'AGYTYP', 'AGYTYPT', 'AGY', 'AGYT', 'AGYSUB', 'AGYSUBT', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'agytyp', 'agytyp_desc', 'agy_code', 'agy_desc', 'agysub_code', 'agysub_desc', 'raw_data']
                    df_clean = df_clean.drop_duplicates(subset=['dataset_key', 'agysub_code'])
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_agency (dataset_key, agytyp, agytyp_desc, agy_code, agy_desc, agysub_code, agysub_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
            
            elif table_name == 'date' and 'DATECODE' in df.columns:
                df_clean = df[['dataset_key', 'DATECODE', 'DATECODET', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'datecode', 'datecode_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_date (dataset_key, datecode, datecode_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'edlvl' and 'EDLVL' in df.columns:
                # Handle education hierarchical structure
                if len(df.columns) >= 5:
                    cols = ['dataset_key', 'EDLVLTYP', 'EDLVLTYPT', 'EDLVL', 'EDLVLT', 'raw_data']
                    if all(col in df.columns for col in cols[1:-1]):
                        df_clean = df[cols].copy()
                        df_clean.columns = ['dataset_key', 'edlvltyp', 'edlvltyp_desc', 'edlvl_code', 'edlvl_desc', 'raw_data']
                    else:
                        df_clean = df[['dataset_key', 'EDLVL', 'EDLVLT', 'raw_data']].copy()
                        df_clean.columns = ['dataset_key', 'edlvl_code', 'edlvl_desc', 'raw_data']
                        df_clean['edlvltyp'] = None
                        df_clean['edlvltyp_desc'] = None
                        df_clean = df_clean[['dataset_key', 'edlvltyp', 'edlvltyp_desc', 'edlvl_code', 'edlvl_desc', 'raw_data']]
                else:
                    df_clean = df[['dataset_key', 'EDLVL', 'EDLVLT', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'edlvl_code', 'edlvl_desc', 'raw_data']
                    df_clean['edlvltyp'] = None
                    df_clean['edlvltyp_desc'] = None
                    df_clean = df_clean[['dataset_key', 'edlvltyp', 'edlvltyp_desc', 'edlvl_code', 'edlvl_desc', 'raw_data']]
                
                df_clean = df_clean[df_clean['edlvl_code'].notna()].copy()
                df_clean = df_clean.drop_duplicates(subset=['dataset_key', 'edlvl_code'])
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_education (dataset_key, edlvltyp, edlvltyp_desc, edlvl_code, edlvl_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'occ' and 'OCC' in df.columns:
                # Handle occupation hierarchical structure
                cols = ['dataset_key', 'OCCTYP', 'OCCTYPT', 'OCCFAM', 'OCCFAMT', 'OCC', 'OCCT', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'occtyp', 'occtyp_desc', 'occfam', 'occfam_desc', 'occ_code', 'occ_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_occupation (dataset_key, occtyp, occtyp_desc, occfam, occfam_desc, occ_code, occ_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                    
            elif table_name == 'loc' and 'LOC' in df.columns:
                # Handle location hierarchical structure
                if len(df.columns) >= 5:
                    cols = ['dataset_key', 'LOCTYP', 'LOCTYPT', 'LOC', 'LOCT', 'raw_data']
                    if all(col in df.columns for col in cols[1:-1]):
                        df_clean = df[cols].copy()
                        df_clean.columns = ['dataset_key', 'loctyp', 'loctyp_desc', 'loc_code', 'loc_desc', 'raw_data']
                    else:
                        df_clean = df[['dataset_key', 'LOC', 'LOCT', 'raw_data']].copy()
                        df_clean.columns = ['dataset_key', 'loc_code', 'loc_desc', 'raw_data']
                        df_clean['loctyp'] = None
                        df_clean['loctyp_desc'] = None
                        df_clean = df_clean[['dataset_key', 'loctyp', 'loctyp_desc', 'loc_code', 'loc_desc', 'raw_data']]
                else:
                    df_clean = df[['dataset_key', 'LOC', 'LOCT', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'loc_code', 'loc_desc', 'raw_data']
                    df_clean['loctyp'] = None
                    df_clean['loctyp_desc'] = None
                    df_clean = df_clean[['dataset_key', 'loctyp', 'loctyp_desc', 'loc_code', 'loc_desc', 'raw_data']]
                    
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_location (dataset_key, loctyp, loctyp_desc, loc_code, loc_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'gsegrd' and 'GSEGRD' in df.columns:
                # Some files have only GSEGRD column, others have description
                if 'GSEGRDT' in df.columns:
                    df_clean = df[['dataset_key', 'GSEGRD', 'GSEGRDT', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'gsegrd_code', 'gsegrd_desc', 'raw_data']
                else:
                    df_clean = df[['dataset_key', 'GSEGRD', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'gsegrd_code', 'raw_data']
                    df_clean['gsegrd_desc'] = None
                    df_clean = df_clean[['dataset_key', 'gsegrd_code', 'gsegrd_desc', 'raw_data']]
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_gsegrd (dataset_key, gsegrd_code, gsegrd_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'loslvl' and 'LOSLVL' in df.columns:
                df_clean = df[['dataset_key', 'LOSLVL', 'LOSLVLT', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'loslvl_code', 'loslvl_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_loslvl (dataset_key, loslvl_code, loslvl_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'patco' and 'PATCO' in df.columns:
                df_clean = df[['dataset_key', 'PATCO', 'PATCOT', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'patco_code', 'patco_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_patco (dataset_key, patco_code, patco_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'pp' and 'PP' in df.columns:
                # Handle different PP file formats
                if 'PPT' in df.columns:
                    df_clean = df[['dataset_key', 'PP', 'PPT', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'pp_code', 'pp_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_payplan (dataset_key, pp_code, pp_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                elif 'PP_AGGT' in df.columns:
                    # Some files have PP_AGG structure instead - skip for now
                    logger.info(f"  Skipping PP file with PP_AGG structure - columns: {list(df.columns)}")
                    records_loaded = 0
                else:
                    df_clean = df[['dataset_key', 'PP', 'raw_data']].copy()
                    df_clean.columns = ['dataset_key', 'pp_code', 'raw_data']
                    df_clean['pp_desc'] = None
                    df_clean = df_clean[['dataset_key', 'pp_code', 'pp_desc', 'raw_data']]
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_payplan (dataset_key, pp_code, pp_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                
            elif table_name == 'ppgrd' and 'PPGRD' in df.columns:
                # Handle pay plan grade hierarchical structure
                if len(df.columns) >= 8:  # Full hierarchical structure
                    cols = ['dataset_key', 'PPTYP', 'PPTYPT', 'PPGROUP', 'PPGROUPT', 'PAYPLAN', 'PAYPLANT', 'PPGRD', 'raw_data']
                    if all(col in df.columns for col in cols[1:-1]):
                        df_clean = df[cols].copy()
                        df_clean.columns = ['dataset_key', 'pptyp', 'pptyp_desc', 'ppgroup', 'ppgroup_desc', 'payplan', 'payplan_desc', 'ppgrd_code', 'raw_data']
                        df_clean['ppgrd_desc'] = None  # No description column for ppgrd_code
                        df_clean = df_clean[['dataset_key', 'pptyp', 'pptyp_desc', 'ppgroup', 'ppgroup_desc', 'payplan', 'payplan_desc', 'ppgrd_code', 'ppgrd_desc', 'raw_data']]
                        
                        data_tuples = [tuple(row) for row in df_clean.values]
                        execute_batch(cur, 
                            "INSERT INTO fedscope_lookup_ppgrd (dataset_key, pptyp, pptyp_desc, ppgroup, ppgroup_desc, payplan, payplan_desc, ppgrd_code, ppgrd_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                            data_tuples, page_size=batch_size)
                        records_loaded = len(df_clean)
                        
            elif table_name == 'sallvl' and 'SALLVL' in df.columns:
                df_clean = df[['dataset_key', 'SALLVL', 'SALLVLT', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'sallvl_code', 'sallvl_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_salary_level (dataset_key, sallvl_code, sallvl_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
                
            elif table_name == 'stemocc' and 'STEMOCC' in df.columns:
                # Handle STEM occupation hierarchical structure
                cols = ['dataset_key', 'STEMAGG', 'STEMAGGT', 'STEMTYP', 'STEMTYPT', 'STEMOCC', 'STEMOCCT', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'stemagg', 'stemagg_desc', 'stemtyp', 'stemtyp_desc', 'stemocc_code', 'stemocc_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_stemocc (dataset_key, stemagg, stemagg_desc, stemtyp, stemtyp_desc, stemocc_code, stemocc_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                    
            elif table_name == 'super' and 'SUPERVIS' in df.columns:
                # Handle supervisory hierarchical structure
                cols = ['dataset_key', 'SUPERTYP', 'SUPERTYPT', 'SUPERVIS', 'SUPERVIST', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'supertyp', 'supertyp_desc', 'supervis_code', 'supervis_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_supervisory (dataset_key, supertyp, supertyp_desc, supervis_code, supervis_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                    
            elif table_name == 'toa' and 'TOA' in df.columns:
                # Handle type of appointment hierarchical structure
                cols = ['dataset_key', 'TOATYP', 'TOATYPT', 'TOA', 'TOAT', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'toatyp', 'toatyp_desc', 'toa_code', 'toa_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_appointment (dataset_key, toatyp, toatyp_desc, toa_code, toa_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                    
            elif table_name == 'wrksch' and 'WORKSCH' in df.columns:
                # Handle work schedule hierarchical structure
                cols = ['dataset_key', 'WSTYP', 'WSTYPT', 'WORKSCH', 'WORKSCHT', 'raw_data']
                if all(col in df.columns for col in cols[1:-1]):
                    df_clean = df[cols].copy()
                    df_clean.columns = ['dataset_key', 'wstyp', 'wstyp_desc', 'worksch_code', 'worksch_desc', 'raw_data']
                    
                    data_tuples = [tuple(row) for row in df_clean.values]
                    execute_batch(cur, 
                        "INSERT INTO fedscope_lookup_work_schedule (dataset_key, wstyp, wstyp_desc, worksch_code, worksch_desc, raw_data) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                        data_tuples, page_size=batch_size)
                    records_loaded = len(df_clean)
                    
            elif table_name == 'wkstat' and 'WORKSTAT' in df.columns:
                df_clean = df[['dataset_key', 'WORKSTAT', 'WORKSTATT', 'raw_data']].copy()
                df_clean.columns = ['dataset_key', 'workstat_code', 'workstat_desc', 'raw_data']
                
                data_tuples = [tuple(row) for row in df_clean.values]
                execute_batch(cur, 
                    "INSERT INTO fedscope_lookup_work_status (dataset_key, workstat_code, workstat_desc, raw_data) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                    data_tuples, page_size=batch_size)
                records_loaded = len(df_clean)
            
            else:
                logger.warning(f"Unhandled table type: {table_name} with columns {list(df.columns)}")
                
            pg_conn.commit()
            cur.close()
            
            if records_loaded > 0:
                logger.info(f"✓ Loaded {records_loaded} records from {table_name}")
            
            return records_loaded
            
        except Exception as e:
            logger.error(f"Error processing lookup table {table_name} from {file_path}: {e}")
            logger.error(f"DataFrame columns: {list(df.columns)}")
            logger.error(f"DataFrame shape: {df.shape}")
            pg_conn.rollback()
            if 'cur' in locals():
                cur.close()
            return 0
        
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return 0

def load_fact_data_to_postgres(pg_conn, extracted_dir, batch_size=100000):
    """Load fact data directly into PostgreSQL."""
    logger.info("Loading fact data directly to PostgreSQL...")
    
    fact_files = glob.glob(os.path.join(extracted_dir, "**/FACTDATA_*.TXT"), recursive=True)
    logger.info(f"Found {len(fact_files)} fact data files")
    
    id_counter = 1
    total_records = 0
    
    cur = pg_conn.cursor()
    
    for fact_file in fact_files:
        # Get the parent directory to determine dataset key
        parent_dir = os.path.basename(os.path.dirname(fact_file))
        quarter, year = get_quarter_year_from_filename(parent_dir)
        
        if not quarter or not year:
            quarter, year = get_quarter_year_from_filename(fact_file)
        
        if not quarter or not year:
            logger.warning(f"Could not parse quarter/year from {fact_file}")
            continue
        
        dataset_key = f"{year}_{quarter}"
        logger.info(f"Loading: {os.path.basename(fact_file)} (dataset: {dataset_key})")
        
        try:
            # Read in chunks for large files
            chunk_count = 0
            for chunk in pd.read_csv(fact_file, dtype=str, chunksize=batch_size):
                chunk_count += 1
                
                # Add metadata columns
                chunk['id'] = range(id_counter, id_counter + len(chunk))
                chunk['dataset_key'] = dataset_key
                chunk['quarter'] = quarter
                chunk['year'] = year
                chunk['data_source'] = os.path.basename(fact_file)
                
                # Create raw_data column
                original_cols = [col for col in chunk.columns if col not in ['id', 'dataset_key', 'quarter', 'year', 'data_source']]
                chunk['raw_data'] = chunk[original_cols].apply(lambda x: '|'.join([f"{col}:{val}" for col, val in x.items()]), axis=1)
                
                # Clean numeric columns
                chunk['employment'] = pd.to_numeric(chunk.get('EMPLOYMENT', 0), errors='coerce').fillna(0).astype(int)
                
                if 'SALARY' in chunk.columns:
                    chunk['salary'] = chunk['SALARY'].astype(str).str.replace('$', '').str.replace(',', '')
                    chunk['salary'] = pd.to_numeric(chunk['salary'], errors='coerce')
                else:
                    chunk['salary'] = None
                
                if 'LOS' in chunk.columns:
                    chunk['length_of_service'] = pd.to_numeric(chunk['LOS'], errors='coerce')
                else:
                    chunk['length_of_service'] = None
                
                # Select final columns
                df_final = chunk[['id', 'dataset_key', 'quarter', 'year', 'raw_data', 'employment', 'salary', 'length_of_service', 'data_source']]
                
                # Insert into PostgreSQL with better batch handling
                data_tuples = [tuple(row) for row in df_final.values]
                logger.info(f"  Inserting batch of {len(data_tuples):,} records...")
                execute_batch(cur, 
                    """INSERT INTO fedscope_employment_facts 
                       (id, dataset_key, quarter, year, raw_data, employment, salary, length_of_service, data_source) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                    data_tuples, page_size=batch_size)
                
                id_counter += len(chunk)
                total_records += len(chunk)
                
                if chunk_count % 5 == 0:
                    logger.info(f"  Processed {chunk_count * batch_size:,} records...")
                    pg_conn.commit()  # Commit periodically
            
            logger.info(f"✓ Loaded {total_records:,} records from {dataset_key}")
            pg_conn.commit()
            
        except Exception as e:
            logger.error(f"✗ Error loading {fact_file}: {e}")
            logger.error(f"Dataset: {dataset_key}, File size: {os.path.getsize(fact_file):,} bytes")
            pg_conn.rollback()
            continue
    
    cur.close()
    logger.info(f"Total fact records loaded: {total_records:,}")

def load_all_lookups_to_postgres(pg_conn, extracted_dir):
    """Load all lookup tables directly to PostgreSQL."""
    logger.info("Loading all lookup tables directly to PostgreSQL...")
    
    # Find all directories with FedScope data
    pattern = os.path.join(extracted_dir, "FedScope_Employment_*")
    fedscope_dirs = glob.glob(pattern)
    
    logger.info(f"Found {len(fedscope_dirs)} FedScope datasets")
    
    total_records = 0
    
    # Load all files
    for fedscope_dir in fedscope_dirs:
        dir_name = os.path.basename(fedscope_dir)
        quarter, year = get_quarter_year_from_filename(dir_name)
        
        if not quarter or not year:
            logger.warning(f"Could not parse quarter/year from {dir_name}")
            continue
        
        dataset_key = f"{year}_{quarter}"
        logger.info(f"Processing dataset: {dataset_key} ({dir_name})")
        
        # Find all lookup files in this directory
        lookup_files = glob.glob(os.path.join(fedscope_dir, "DT*.txt"))
        
        dataset_records = 0
        for lookup_file in lookup_files:
            records = load_lookup_file_to_postgres(pg_conn, lookup_file, dataset_key)
            dataset_records += records
        
        logger.info(f"Dataset total: {dataset_records:,} records")
        total_records += dataset_records
    
    logger.info(f"Total lookup records loaded: {total_records:,}")
    return total_records

def main():
    """Main function to load FedScope data directly to PostgreSQL."""
    
    # Setup
    extracted_dir = "fedscope_data/extracted"
    
    if not os.path.exists(extracted_dir):
        logger.error(f"Extracted data directory {extracted_dir} not found.")
        return
    
    # Connect to PostgreSQL
    conn_str = os.getenv('DATABASE_URL')
    if not conn_str:
        logger.error("DATABASE_URL environment variable not set")
        logger.error("Please set DATABASE_URL in your .env file")
        return
    
    try:
        pg_conn = psycopg2.connect(conn_str)
        logger.info("Connected to PostgreSQL")
        
        # Create schema
        create_postgres_schema(pg_conn)
        
        # Load lookup data
        lookup_records = load_all_lookups_to_postgres(pg_conn, extracted_dir)
        
        # Load fact data
        load_fact_data_to_postgres(pg_conn, extracted_dir)
        
        # Create indexes for performance
        create_indexes(pg_conn)
        
        # Show summary statistics
        logger.info("\n" + "="*60)
        logger.info("POSTGRESQL LOADING COMPLETE")
        logger.info("="*60)
        
        cur = pg_conn.cursor()
        
        # Show table counts
        tables = [
            'fedscope_employment_facts', 'fedscope_lookup_agelvl', 'fedscope_lookup_agency', 
            'fedscope_lookup_date', 'fedscope_lookup_education', 'fedscope_lookup_location',
            'fedscope_lookup_occupation'
        ]
        
        logger.info("\nTable Summary:")
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                logger.info(f"  {table}: {count:,} records")
            except Exception as e:
                logger.warning(f"  {table}: Error getting count - {e}")
        
        cur.close()
        logger.info("\nData successfully loaded directly to PostgreSQL!")
        
    except Exception as e:
        logger.error(f"Error during PostgreSQL loading: {e}")
        raise
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()

if __name__ == "__main__":
    main()