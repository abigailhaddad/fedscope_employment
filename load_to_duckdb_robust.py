#!/usr/bin/env python3
"""
Robust DuckDB loader that handles schema changes across years.
Creates one massive denormalized table with all data concatenated across years.
"""

import duckdb
import pandas as pd
import os
import re
import glob
from pathlib import Path
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"

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

def analyze_schema(fact_file):
    """Analyze the schema of a fact file."""
    try:
        # Read just the header to understand schema
        df_sample = pd.read_csv(fact_file, sep=',', encoding='latin-1', nrows=0)
        columns = list(df_sample.columns)
        logger.info(f"    Schema: {len(columns)} columns: {', '.join(columns)}")
        return columns
    except Exception as e:
        logger.error(f"    Error analyzing schema: {e}")
        return None

def load_lookup_tables(conn, data_dir, dataset_key):
    """Load lookup tables for a dataset."""
    logger.info(f"  Loading lookup tables for {dataset_key}...")
    
    # Lookup table mapping
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
                
                # Check if table exists, create if not
                tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'").fetchall()
                
                if not tables:
                    # Create table with flexible schema
                    conn.register('df_temp_lookup', df)
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_temp_lookup LIMIT 0")
                    conn.unregister('df_temp_lookup')
                
                # Get existing columns
                existing_columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
                
                # Add missing columns
                for col in df.columns:
                    if col not in existing_columns:
                        try:
                            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR")
                        except:
                            pass
                
                # Align dataframe columns with table
                all_table_columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
                for col in all_table_columns:
                    if col not in df.columns:
                        df[col] = None
                df = df.reindex(columns=all_table_columns)
                
                # Insert data
                temp_name = f"df_lookup_{table_name}_{dataset_key.replace('_', '')}"
                conn.register(temp_name, df)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_name}")
                conn.unregister(temp_name)
                
                logger.info(f"    Loaded {len(df)} rows into {table_name}")
                
            except Exception as e:
                logger.error(f"    Error loading {table_name}: {e}")
        else:
            logger.warning(f"  Lookup file not found: {file_path}")
            # Create empty table for missing lookup files (especially payplan for older datasets)
            if table_name == 'payplan':
                try:
                    tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'").fetchall()
                    if not tables:
                        conn.execute(f"""
                            CREATE TABLE {table_name} (
                                dataset_key VARCHAR,
                                pp VARCHAR,
                                ppt VARCHAR
                            )
                        """)
                        logger.info(f"    Created empty {table_name} table for schema compatibility")
                except Exception as e:
                    logger.error(f"    Error creating empty {table_name} table: {e}")

def detect_and_log_duplicates(conn):
    """Detect duplicates in lookup tables and log them to a file."""
    logger.info("Checking for duplicate keys in lookup tables...")
    
    import json
    duplicates_log = {}
    
    # Check each lookup table for duplicates
    lookup_tables = [
        ('agency', 'agysub'),
        ('education', 'edlvl'),
        ('occupation', 'occ'),
        ('location', 'loc'),
        ('ppgrd', 'ppgrd'),
        ('agelvl', 'agelvl'),
        ('gsegrd', 'gsegrd'),
        ('loslvl', 'loslvl'),
        ('patco', 'patco'),
        ('salary_level', 'sallvl'),
        ('stemocc', 'stemocc'),
        ('supervisory', 'supervis'),
        ('appointment', 'toa'),
        ('work_schedule', 'worksch'),
        ('work_status', 'workstat')
    ]
    
    for table, key_col in lookup_tables:
        # Find duplicates
        duplicates = conn.execute(f"""
            SELECT dataset_key, {key_col}, COUNT(*) as cnt
            FROM {table}
            WHERE {key_col} IS NOT NULL
            GROUP BY dataset_key, {key_col}
            HAVING COUNT(*) > 1
            ORDER BY dataset_key, {key_col}
        """).fetchall()
        
        if duplicates:
            if table not in duplicates_log:
                duplicates_log[table] = []
                
            for dataset_key, code, count in duplicates:
                # Get all duplicate records
                records = conn.execute(f"""
                    SELECT * FROM {table}
                    WHERE dataset_key = '{dataset_key}' AND {key_col} = '{code}'
                    ORDER BY ROWID
                """).fetchall()
                
                # We'll keep the first one (lowest ROWID)
                kept_record = records[0]
                discarded_records = records[1:]
                
                duplicate_entry = {
                    'dataset_key': dataset_key,
                    'code': code,
                    'count': count,
                    'kept': dict(zip([desc[0] for desc in conn.description], kept_record)),
                    'discarded': [dict(zip([desc[0] for desc in conn.description], rec)) for rec in discarded_records]
                }
                
                duplicates_log[table].append(duplicate_entry)
                logger.info(f"  Found duplicate in {table}: {dataset_key}/{code} ({count} occurrences)")
    
    # Save to file
    if duplicates_log:
        with open('lookup_duplicates_log.json', 'w') as f:
            json.dump(duplicates_log, f, indent=2)
        logger.info(f"\nDuplicate records logged to lookup_duplicates_log.json")
        
        # Create a human-readable summary
        with open('lookup_duplicates_summary.txt', 'w') as f:
            f.write("FedScope Lookup Table Duplicates Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write("This file documents cases where lookup tables contain multiple records\n")
            f.write("for the same code within a dataset. When joining, we use the first\n")
            f.write("occurrence (by ROWID) and discard the others.\n\n")
            
            total_duplicates = sum(len(dups) for dups in duplicates_log.values())
            f.write(f"SUMMARY: Found {total_duplicates} duplicate codes across {len(duplicates_log)} tables\n")
            f.write("Most duplicates are from early years (1998-2003) when agencies were renamed.\n\n")
            
            for table, dups in duplicates_log.items():
                f.write(f"\n{'='*20} {table.upper()} TABLE {'='*20}\n")
                f.write(f"Found {len(dups)} duplicate codes in this table\n\n")
                
                for dup in dups:
                    f.write(f"Dataset: {dup['dataset_key']} | Code: '{dup['code']}' | {dup['count']} duplicates\n")
                    f.write("-" * 60 + "\n")
                    
                    kept = dup['kept']
                    f.write("✅ KEPT (first occurrence):\n")
                    if table == 'agency':
                        f.write(f"   Agency: {kept.get('agyt', 'N/A')}\n")
                        f.write(f"   Sub-Agency: {kept.get('agysubt', 'N/A')}\n")
                    else:
                        # Show the description field for other tables
                        desc_key = [k for k in kept.keys() if k.endswith('t') and k != 'dataset_key'][0] if any(k.endswith('t') for k in kept.keys()) else None
                        if desc_key:
                            f.write(f"   Description: {kept.get(desc_key, 'N/A')}\n")
                    
                    f.write("\n❌ DISCARDED:\n")
                    for i, disc in enumerate(dup['discarded'], 1):
                        if table == 'agency':
                            f.write(f"   #{i}: Agency: {disc.get('agyt', 'N/A')}\n")
                            f.write(f"       Sub-Agency: {disc.get('agysubt', 'N/A')}\n")
                        else:
                            desc_key = [k for k in disc.keys() if k.endswith('t') and k != 'dataset_key'][0] if any(k.endswith('t') for k in disc.keys()) else None
                            if desc_key:
                                f.write(f"   #{i}: Description: {disc.get(desc_key, 'N/A')}\n")
                    f.write("\n")
        
        logger.info("Human-readable summary saved to lookup_duplicates_summary.txt")
    
    return duplicates_log

def create_denormalized_records_for_dataset(conn, dataset_key):
    """Create denormalized records for a specific dataset."""
    
    # Check what columns exist in employment_facts_all for this dataset
    fact_columns = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'employment_facts_all'").fetchall()
    fact_column_names = [col[0] for col in fact_columns]
    
    # Check if denormalized table exists, create if not
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'employment_denormalized'").fetchall()
    
    if not tables:
        logger.info("    Creating employment_denormalized table...")
        conn.execute("""
        CREATE TABLE employment_denormalized (
            dataset_key VARCHAR,
            quarter VARCHAR,
            year INTEGER,
            agysub VARCHAR,
            loc VARCHAR,
            agelvl VARCHAR,
            edlvl VARCHAR,
            gsegrd VARCHAR,
            loslvl VARCHAR,
            occ VARCHAR,
            patco VARCHAR,
            pp VARCHAR,
            ppgrd VARCHAR,
            sallvl VARCHAR,
            stemocc VARCHAR,
            supervis VARCHAR,
            toa VARCHAR,
            wrksch VARCHAR,
            wkstat VARCHAR,
            datecode VARCHAR,
            employment VARCHAR,
            salary VARCHAR,
            los VARCHAR,
            agelvlt VARCHAR,
            agy VARCHAR,
            agysubt VARCHAR,
            edlvlt VARCHAR,
            gsegrdt VARCHAR,
            loct VARCHAR,
            loslvlt VARCHAR,
            occfam VARCHAR,
            occt VARCHAR,
            occfamt VARCHAR,
            patcot VARCHAR,
            ppt VARCHAR,
            ppgrdt VARCHAR,
            sallvlt VARCHAR,
            stemocct VARCHAR,
            supervist VARCHAR,
            toat VARCHAR,
            wrkscht VARCHAR,
            wkstatt VARCHAR
        )
        """)
    
    # Build dynamic SQL based on available columns
    if "pp" in fact_column_names:
        pp_field = "CASE WHEN f.pp LIKE '*%' THEN NULL ELSE f.pp END"
    else:
        pp_field = "NULL"
    
    los_field = "f.los" if "los" in fact_column_names else "NULL"
    
    # Build payplan join conditionally
    if "pp" in fact_column_names:
        payplan_join = "LEFT JOIN (SELECT DISTINCT ON (dataset_key, pp) * FROM payplan ORDER BY dataset_key, pp, ROWID) pl ON f.dataset_key = pl.dataset_key AND f.pp = pl.pp"
    else:
        payplan_join = f"LEFT JOIN (SELECT dataset_key, NULL as pp, NULL as ppt FROM (SELECT DISTINCT dataset_key FROM employment_facts_all WHERE dataset_key = '{dataset_key}')) pl ON f.dataset_key = pl.dataset_key"
    
    # Insert denormalized records for this dataset
    conn.execute(f"""
    INSERT INTO employment_denormalized
    SELECT 
        f.dataset_key,
        f.quarter,
        f.year,
        
        -- Original fact columns in same order as table (clean asterisks)
        f.agysub,
        f.loc,
        CASE WHEN f.agelvl LIKE '*%' THEN NULL ELSE f.agelvl END as agelvl,
        CASE WHEN f.edlvl LIKE '*%' THEN NULL ELSE f.edlvl END as edlvl,
        CASE WHEN f.gsegrd LIKE '*%' THEN NULL ELSE f.gsegrd END as gsegrd,
        f.loslvl,
        CASE WHEN f.occ LIKE '*%' THEN NULL ELSE f.occ END as occ,
        f.patco,
        {pp_field} as pp,
        CASE WHEN f.ppgrd LIKE '*%' THEN NULL ELSE f.ppgrd END as ppgrd,
        f.sallvl,
        CASE WHEN f.stemocc LIKE '*%' THEN NULL ELSE f.stemocc END as stemocc,
        CASE WHEN f.supervis LIKE '*%' THEN NULL ELSE f.supervis END as supervis,
        CASE WHEN f.toa LIKE '*%' THEN NULL ELSE f.toa END as toa,
        CASE WHEN f.worksch LIKE '*%' THEN NULL ELSE f.worksch END as wrksch,
        f.workstat as wkstat,
        f.datecode,
        f.employment,
        CASE WHEN f.salary LIKE '*%' THEN NULL ELSE f.salary END as salary,
        {los_field} as los,
        
        -- Lookup descriptions with original column names
        a.agelvlt,
        ag.agy,
        ag.agysubt,
        e.edlvlt,
        g.gsegrd as gsegrdt,
        l.loct,
        los.loslvlt,
        o.occ as occfam,
        o.occt,
        o.occfamt,
        p.patcot,
        pl.ppt,
        pp.ppgrd as ppgrdt,
        sl.sallvlt,
        st.stemocct,
        s.supervist,
        t.toat,
        ws.workscht as wrkscht,
        wst.workstatt as wkstatt
        
    FROM employment_facts_all f
    -- Use deduplicated lookup tables (taking first occurrence by ROWID)
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, agelvl) * FROM agelvl ORDER BY dataset_key, agelvl, ROWID) a 
        ON f.dataset_key = a.dataset_key AND f.agelvl = a.agelvl
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, edlvl) * FROM education ORDER BY dataset_key, edlvl, ROWID) e 
        ON f.dataset_key = e.dataset_key AND f.edlvl = e.edlvl  
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, gsegrd) * FROM gsegrd ORDER BY dataset_key, gsegrd, ROWID) g 
        ON f.dataset_key = g.dataset_key AND f.gsegrd = g.gsegrd
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, loslvl) * FROM loslvl ORDER BY dataset_key, loslvl, ROWID) los 
        ON f.dataset_key = los.dataset_key AND f.loslvl = los.loslvl
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, occ) * FROM occupation ORDER BY dataset_key, occ, ROWID) o 
        ON f.dataset_key = o.dataset_key AND f.occ = o.occ
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, patco) * FROM patco ORDER BY dataset_key, patco, ROWID) p 
        ON f.dataset_key = p.dataset_key AND f.patco = p.patco
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, ppgrd) * FROM ppgrd ORDER BY dataset_key, ppgrd, ROWID) pp 
        ON f.dataset_key = pp.dataset_key AND f.ppgrd = pp.ppgrd
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, sallvl) * FROM salary_level ORDER BY dataset_key, sallvl, ROWID) sl 
        ON f.dataset_key = sl.dataset_key AND f.sallvl = sl.sallvl
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, stemocc) * FROM stemocc ORDER BY dataset_key, stemocc, ROWID) st 
        ON f.dataset_key = st.dataset_key AND f.stemocc = st.stemocc
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, supervis) * FROM supervisory ORDER BY dataset_key, supervis, ROWID) s 
        ON f.dataset_key = s.dataset_key AND f.supervis = s.supervis
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, toa) * FROM appointment ORDER BY dataset_key, toa, ROWID) t 
        ON f.dataset_key = t.dataset_key AND f.toa = t.toa
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, worksch) * FROM work_schedule ORDER BY dataset_key, worksch, ROWID) ws 
        ON f.dataset_key = ws.dataset_key AND f.worksch = ws.worksch
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, workstat) * FROM work_status ORDER BY dataset_key, workstat, ROWID) wst 
        ON f.dataset_key = wst.dataset_key AND f.workstat = wst.workstat
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, agysub) * FROM agency ORDER BY dataset_key, agysub, ROWID) ag 
        ON f.dataset_key = ag.dataset_key AND f.agysub = ag.agysub
    LEFT JOIN (SELECT DISTINCT ON (dataset_key, loc) * FROM location ORDER BY dataset_key, loc, ROWID) l 
        ON f.dataset_key = l.dataset_key AND f.loc = l.loc
    {payplan_join}
    WHERE f.dataset_key = '{dataset_key}'
    """)
    
    # Get count for this dataset
    result = conn.execute(f"SELECT COUNT(*) FROM employment_denormalized WHERE dataset_key = '{dataset_key}'").fetchone()
    logger.info(f"    ✅ Created {result[0]:,} denormalized records for {dataset_key}")

def load_single_dataset_robust(conn, dataset_path, dataset_key, month, year):
    """Load a single dataset into DuckDB, handling schema variations."""
    logger.info(f"Loading {dataset_key} from {os.path.basename(dataset_path)}...")
    
    # Find the actual subdirectory containing the data files
    data_dirs = []
    for root, dirs, files in os.walk(dataset_path):
        if any(f.upper().startswith('FACTDATA') for f in files):
            data_dirs.append(root)
    
    if not data_dirs:
        logger.warning(f"  No FACTDATA files found in {dataset_path}")
        return 0
    
    data_dir = data_dirs[0]
    
    # Load fact data using pandas to handle schema differences
    fact_files = glob.glob(os.path.join(data_dir, "FACTDATA*.TXT"))
    if not fact_files:
        logger.warning(f"  No fact data files found")
        return 0
    
    fact_file = fact_files[0]
    
    try:
        # Analyze schema first
        columns = analyze_schema(fact_file)
        if not columns:
            return 0
        
        # Read the data with pandas
        logger.info(f"  Reading CSV with pandas...")
        df = pd.read_csv(fact_file, sep=',', encoding='latin-1', dtype=str)
        
        # Add metadata columns
        df.insert(0, 'dataset_key', dataset_key)
        df.insert(1, 'quarter', month)
        df.insert(2, 'year', year)
        
        # Clean column names
        df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
        
        logger.info(f"  Loaded {len(df):,} records with {len(df.columns)} columns")
        
        # Check if main table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'employment_facts_all'").fetchall()
        
        if not tables:
            # Create the main table with flexible schema
            logger.info("  Creating main employment_facts_all table")
            conn.register('df_temp', df)
            conn.execute("CREATE TABLE employment_facts_all AS SELECT * FROM df_temp LIMIT 0")
            conn.unregister('df_temp')
        
        # Get existing columns in the table if it exists
        try:
            existing_columns = [row[0] for row in conn.execute("PRAGMA table_info(employment_facts_all)").fetchall()]
        except:
            existing_columns = []
        
        df_columns = list(df.columns)
        
        # Add missing columns to the table
        for col in df_columns:
            if col not in existing_columns:
                try:
                    conn.execute(f"ALTER TABLE employment_facts_all ADD COLUMN {col} VARCHAR")
                    logger.info(f"    Added new column: {col}")
                except:
                    pass  # Column might already exist
        
        # Get all columns from the target table in order
        all_table_columns = [row[1] for row in conn.execute("PRAGMA table_info(employment_facts_all)").fetchall()]
        
        # Ensure dataframe has all required columns, add missing ones as NULL
        for col in all_table_columns:
            if col not in df.columns:
                df[col] = None
                logger.info(f"    Added missing column {col} as NULL")
        
        # Reorder dataframe columns to match table schema exactly
        df = df.reindex(columns=all_table_columns)
        
        # Use a unique registration name for each dataset
        temp_table_name = f"df_temp_{dataset_key.replace('_', '')}"
        
        # Insert data using pandas integration
        conn.register(temp_table_name, df)
        
        # Insert the data
        conn.execute(f"INSERT INTO employment_facts_all SELECT * FROM {temp_table_name}")
        conn.unregister(temp_table_name)
        
        logger.info(f"  Successfully loaded {len(df):,} records")
        return len(df)
        
    except Exception as e:
        logger.error(f"  Error loading fact data: {e}")
        return 0

def load_all_data_robust():
    """Load all extracted FedScope data into DuckDB with robust schema handling."""
    extracted_dir = "fedscope_data/extracted"
    
    if not os.path.exists(extracted_dir):
        logger.error(f"Extracted data directory not found: {extracted_dir}")
        logger.info("Please run fix_and_extract.py first")
        return
    
    # Connect to DuckDB
    conn = duckdb.connect(DUCKDB_PATH)
    
    # Drop all existing tables to start fresh
    logger.info("Dropping existing tables to start fresh...")
    try:
        conn.execute("DROP TABLE IF EXISTS employment_facts_all")
        conn.execute("DROP TABLE IF EXISTS employment_denormalized")
        # Drop lookup tables
        lookup_tables = ['agelvl', 'agency', 'date', 'education', 'gsegrd', 'location', 
                        'loslvl', 'occupation', 'patco', 'payplan', 'ppgrd', 'salary_level', 
                        'stemocc', 'supervisory', 'appointment', 'work_schedule', 'work_status']
        for table in lookup_tables:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    except:
        pass
    
    loaded_datasets_set = set()
    
    # Get all dataset directories
    dataset_dirs = sorted([d for d in os.listdir(extracted_dir) 
                          if os.path.isdir(os.path.join(extracted_dir, d))])
    
    logger.info(f"Found {len(dataset_dirs)} datasets to load")
    
    total_records = 0
    loaded_datasets = 0
    
    # Process each dataset
    for i, dataset_dir in enumerate(dataset_dirs, 1):
        month, year = get_quarter_year_from_filename(dataset_dir)
        if not month or not year:
            logger.warning(f"Could not determine period for {dataset_dir}, skipping")
            continue
        
        dataset_key = f"{year}_{month}"
        dataset_path = os.path.join(extracted_dir, dataset_dir)
        
        logger.info(f"\n[{i}/{len(dataset_dirs)}] Processing {dataset_key}...")
        
        # Load lookup tables for this dataset
        data_dirs = []
        for root, dirs, files in os.walk(dataset_path):
            if any(f.upper().startswith('DT') for f in files):
                data_dirs.append(root)
        
        if data_dirs:
            load_lookup_tables(conn, data_dirs[0], dataset_key)
        
        records = load_single_dataset_robust(conn, dataset_path, dataset_key, month, year)
        if records > 0:
            total_records += records
            loaded_datasets += 1
            loaded_datasets_set.add(dataset_key)  # Track newly loaded
            logger.info(f"  ✅ Success: {records:,} records loaded")
            
            # Immediately create denormalized records for this dataset
            logger.info(f"  🔗 Creating denormalized records for {dataset_key}...")
            create_denormalized_records_for_dataset(conn, dataset_key)
            
        else:
            logger.warning(f"  ⚠️ No records loaded for {dataset_key}")
            logger.error(f"  ❌ Stopping due to failed dataset: {dataset_key}")
            raise Exception(f"No records loaded for dataset {dataset_key}")
    
    # Detect and log duplicates after all data is loaded
    logger.info("\nDetecting and logging duplicate lookup entries...")
    detect_and_log_duplicates(conn)
    
    # Create final indexes
    logger.info("\nCreating final indexes...")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_denorm_year ON employment_denormalized(year)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_denorm_quarter ON employment_denormalized(quarter)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_denorm_dataset ON employment_denormalized(dataset_key)")
        
        # Get final statistics
        result = conn.execute("SELECT COUNT(*) FROM employment_denormalized").fetchone()
        final_count = result[0]
        
        # Year range
        year_range = conn.execute("""
            SELECT MIN(CAST(year AS INTEGER)) as min_year, 
                   MAX(CAST(year AS INTEGER)) as max_year, 
                   COUNT(DISTINCT year) as num_years
            FROM employment_denormalized
        """).fetchone()
        
        logger.info(f"\nData loading complete!")
        logger.info(f"Loaded datasets: {loaded_datasets}/{len(dataset_dirs)}")
        logger.info(f"Total records: {final_count:,}")
        logger.info(f"Year range: {year_range[0]} to {year_range[1]} ({year_range[2]} unique years)")
        
    except Exception as e:
        logger.error(f"Error creating indexes or getting final statistics: {e}")
    
    conn.close()

if __name__ == "__main__":
    load_all_data_robust()