#!/usr/bin/env python3
"""
Validate the DuckDB database to ensure all data was loaded correctly.
"""

import duckdb
import os
import glob
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_PATH = "fedscope_employment.duckdb"

def count_raw_files():
    """Count records in all raw FACTDATA files."""
    logger.info("Counting records in raw files...")
    
    extracted_dir = "fedscope_data/extracted"
    total_raw_records = 0
    dataset_counts = {}
    
    if not os.path.exists(extracted_dir):
        logger.error(f"Extracted data directory not found: {extracted_dir}")
        return 0, {}
    
    # Get all dataset directories
    dataset_dirs = sorted([d for d in os.listdir(extracted_dir) 
                          if os.path.isdir(os.path.join(extracted_dir, d))])
    
    for dataset_dir in dataset_dirs:
        dataset_path = os.path.join(extracted_dir, dataset_dir)
        
        # Find FACTDATA files
        fact_files = []
        for root, dirs, files in os.walk(dataset_path):
            for file in files:
                if file.upper().startswith('FACTDATA'):
                    fact_files.append(os.path.join(root, file))
        
        if fact_files:
            fact_file = fact_files[0]
            try:
                # Count lines (subtract 1 for header)
                with open(fact_file, 'r', encoding='latin-1') as f:
                    line_count = sum(1 for line in f) - 1
                
                dataset_counts[dataset_dir] = line_count
                total_raw_records += line_count
                logger.info(f"  {dataset_dir}: {line_count:,} records")
                
            except Exception as e:
                logger.error(f"  Error counting {dataset_dir}: {e}")
                dataset_counts[dataset_dir] = 0
        else:
            logger.warning(f"  No FACTDATA file found in {dataset_dir}")
            dataset_counts[dataset_dir] = 0
    
    logger.info(f"Total raw records: {total_raw_records:,}")
    return total_raw_records, dataset_counts

def validate_duckdb():
    """Validate the DuckDB database."""
    if not os.path.exists(DUCKDB_PATH):
        logger.error(f"DuckDB file not found: {DUCKDB_PATH}")
        return False
    
    logger.info("Validating DuckDB database...")
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    try:
        # Check what tables exist
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        logger.info("Tables in database:")
        for (table_name,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"  {table_name}: {count:,} records")
        
        # Check employment_facts_all table first, then employment_facts
        table_to_check = None
        if any(t == 'employment_facts_all' for (t,) in tables):
            table_to_check = 'employment_facts_all'
        elif any(t == 'employment_facts' for (t,) in tables):
            table_to_check = 'employment_facts'
        
        if table_to_check:
            logger.info(f"\nValidating {table_to_check} table:")
            
            # Count total records
            total_db_records = conn.execute(f"SELECT COUNT(*) FROM {table_to_check}").fetchone()[0]
            logger.info(f"  Total records in DB: {total_db_records:,}")
            
            # Count by dataset_key
            dataset_counts_db = conn.execute(f"""
                SELECT dataset_key, COUNT(*) as count
                FROM {table_to_check} 
                GROUP BY dataset_key 
                ORDER BY dataset_key
            """).fetchall()
            
            logger.info("  Records by dataset:")
            for dataset_key, count in dataset_counts_db:
                logger.info(f"    {dataset_key}: {count:,}")
            
            # Year range
            year_range = conn.execute(f"""
                SELECT MIN(CAST(year AS INTEGER)) as min_year, 
                       MAX(CAST(year AS INTEGER)) as max_year, 
                       COUNT(DISTINCT year) as num_years
                FROM {table_to_check}
            """).fetchone()
            logger.info(f"  Year range: {year_range[0]} to {year_range[1]} ({year_range[2]} unique years)")
            
            # Quarter distribution
            quarter_dist = conn.execute(f"""
                SELECT quarter, COUNT(*) as count
                FROM {table_to_check} 
                GROUP BY quarter 
                ORDER BY quarter
            """).fetchall()
            logger.info("  Records by quarter:")
            for quarter, count in quarter_dist:
                logger.info(f"    {quarter}: {count:,}")
            
            return total_db_records, dict(dataset_counts_db)
        
        else:
            logger.error("employment_facts table not found!")
            return 0, {}
            
    except Exception as e:
        logger.error(f"Error validating database: {e}")
        return 0, {}
    finally:
        conn.close()

def compare_counts(raw_total, raw_by_dataset, db_total, db_by_dataset):
    """Compare raw file counts with database counts."""
    logger.info("\n=== VALIDATION SUMMARY ===")
    
    logger.info(f"Raw files total: {raw_total:,}")
    logger.info(f"Database total:  {db_total:,}")
    
    if raw_total == db_total:
        logger.info("✅ Total record counts match!")
    else:
        logger.warning(f"❌ Total counts don't match! Difference: {db_total - raw_total:,}")
    
    # Convert raw dataset names to database keys for proper comparison
    raw_by_key = {}
    for dataset, count in raw_by_dataset.items():
        if dataset.startswith('FedScope_Employment_'):
            parts = dataset.replace('FedScope_Employment_', '').split('_')
            if len(parts) == 2:
                month, year = parts
                dataset_key = f"{year}_{month}"
                raw_by_key[dataset_key] = count
    
    # Compare by dataset key
    logger.info("\nDataset comparison (showing only mismatches if any):")
    all_keys = set(raw_by_key.keys()) | set(db_by_dataset.keys())
    
    mismatches = 0
    matches = 0
    
    for dataset_key in sorted(all_keys):
        raw_count = raw_by_key.get(dataset_key, 0)
        db_count = db_by_dataset.get(dataset_key, 0)
        
        if raw_count == db_count:
            matches += 1
            # Only show matches if there are very few datasets or if requested
            if len(all_keys) <= 10:
                logger.info(f"  ✅ {dataset_key}: {raw_count:,} records")
        else:
            status = "❌"
            mismatches += 1
            logger.warning(f"  {status} {dataset_key}: Raw={raw_count:,}, DB={db_count:,}")
    
    if mismatches == 0:
        logger.info(f"\n✅ All {matches} datasets match perfectly!")
        return True
    else:
        logger.warning(f"\n❌ {mismatches} datasets have mismatched counts, {matches} match correctly")
        return False

def check_for_asterisks(conn):
    """Check for any remaining asterisk values that could break Hugging Face parsing."""
    logger.info("\n=== ASTERISK VALUE CHECK ===")
    logger.info("Checking for asterisk values that could cause parsing errors...")
    
    try:
        # Get all columns from denormalized table
        columns = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'employment_denormalized'").fetchall()
        column_names = [col[0] for col in columns]
        
        total_asterisks = 0
        problematic_fields = []
        
        for col in column_names:
            # Skip known safe fields
            if col in ['dataset_key', 'quarter', 'year', 'employment']:
                continue
                
            # Check for any asterisk patterns
            query = f"""
            SELECT COUNT(*) as count, 
                   {col} as value
            FROM employment_denormalized 
            WHERE {col} LIKE '%*%'
            GROUP BY {col}
            ORDER BY count DESC
            """
            
            try:
                results = conn.execute(query).fetchall()
                if results:
                    for count, value in results:
                        logger.warning(f"  ❌ {col}: {count:,} records with value '{value}'")
                        total_asterisks += count
                        problematic_fields.append((col, value, count))
            except Exception as e:
                # Skip if column can't be queried this way
                pass
        
        if total_asterisks == 0:
            logger.info("  ✅ No asterisk values found - data is clean!")
        else:
            logger.error(f"\n❌ CRITICAL: Found {total_asterisks:,} asterisk values across {len(set(f[0] for f in problematic_fields))} fields")
            logger.error("These MUST be cleaned before uploading to Hugging Face!")
            logger.error("\nProblematic fields:")
            for field, value, count in sorted(problematic_fields, key=lambda x: x[2], reverse=True)[:10]:
                logger.error(f"  - {field}: '{value}' ({count:,} records)")
                
        return total_asterisks == 0
        
    except Exception as e:
        logger.error(f"Error checking for asterisks: {e}")
        return False

def validate_field_completeness(conn):
    """Validate that all expected fields are present and nulls are expected."""
    logger.info("\n=== FIELD COMPLETENESS VALIDATION ===")
    
    try:
        # Expected fields based on original FedScope structure - ALL fields from denormalized table
        expected_fields = {
            # Core identifiers  
            'dataset_key', 'quarter', 'year',
            
            # Original fact table fields (with original names)
            'agysub',        # Sub-agency code
            'loc',           # Location code
            'agelvl',        # Age level code
            'edlvl',         # Education level code
            'gsegrd',        # GS equivalent grade code
            'loslvl',        # Length of service level code
            'occ',           # Occupation code
            'patco',         # PATCO category code
            'pp',            # Pay plan code (null before 2016)
            'ppgrd',         # Pay plan and grade code
            'sallvl',        # Salary level code
            'stemocc',       # STEM occupation indicator
            'supervis',      # Supervisory status code
            'toa',           # Type of appointment code
            'wrksch',        # Work schedule code
            'wkstat',        # Work status code
            'datecode',      # Date code
            'employment',    # Employment count
            'salary',        # Annual salary
            'los',           # Length of service (null before certain years)
            
            # Lookup descriptions (with 't' suffix pattern)
            'agelvlt',       # Age level description
            'agy',           # Agency code (from agency lookup)
            'agysubt',       # Sub-agency description
            'edlvlt',        # Education level description
            'gsegrdt',       # GS equivalent grade description
            'loct',          # Location description
            'loslvlt',       # Length of service level description
            'occfam',        # Occupation family code
            'occt',          # Occupation description
            'occfamt',       # Occupation family description
            'patcot',        # PATCO category description
            'ppt',           # Pay plan description (null before 2016)
            'ppgrdt',        # Pay plan and grade description
            'sallvlt',       # Salary level description
            'stemocct',      # STEM occupation description
            'supervist',     # Supervisory status description
            'toat',          # Type of appointment description
            'wrkscht',       # Work schedule description
            'wkstatt'        # Work status description
        }
        
        # Get actual columns
        columns = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'employment_denormalized'").fetchall()
        actual_fields = {col[0] for col in columns}
        
        # Check missing fields
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        if missing_fields:
            logger.error(f"❌ Missing expected fields: {sorted(missing_fields)}")
        else:
            logger.info("✅ All expected fields are present")
            
        if extra_fields:
            logger.warning(f"⚠️ Unexpected extra fields: {sorted(extra_fields)}")
        
        # Check expected null patterns
        logger.info("\nChecking expected null patterns...")
        
        # PP field should be null before 2016
        pp_null_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE pp IS NULL AND year < 2016").fetchone()[0]
        pp_total_pre2016 = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE year < 2016").fetchone()[0]
        
        if pp_total_pre2016 > 0:
            pp_null_percent = (pp_null_count / pp_total_pre2016) * 100
            logger.info(f"✅ PP field nulls before 2016: {pp_null_count:,}/{pp_total_pre2016:,} ({pp_null_percent:.1f}%)")
        
        # Check for unexpected nulls in core fields that should always be present
        core_fields = ['agelvl', 'edlvl', 'occ', 'patco', 'agysub', 'loc']
        for field in core_fields:
            if field in actual_fields:
                null_count = conn.execute(f"SELECT COUNT(*) FROM employment_denormalized WHERE {field} IS NULL").fetchone()[0]
                if null_count > 0:
                    logger.warning(f"⚠️ Unexpected nulls in {field}: {null_count:,} records")
                else:
                    logger.info(f"✅ {field}: no unexpected nulls")
        
        # Check salary redaction pattern
        salary_null_count = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE salary IS NULL").fetchone()[0]
        total_records = conn.execute("SELECT COUNT(*) FROM employment_denormalized").fetchone()[0]
        salary_null_percent = (salary_null_count / total_records) * 100
        logger.info(f"✅ Salary nulls (redacted): {salary_null_count:,}/{total_records:,} ({salary_null_percent:.2f}%)")
        
        # Check lookup description coverage
        logger.info("\nChecking lookup description coverage...")
        
        # Sample a few key lookup relationships
        lookup_checks = [
            ('agelvl', 'agelvlt'),
            ('edlvl', 'edlvlt'), 
            ('occ', 'occt'),
            ('patco', 'patcot'),
            ('loc', 'loct')
        ]
        
        for code_field, desc_field in lookup_checks:
            if code_field in actual_fields and desc_field in actual_fields:
                # Check records where code exists but description is null
                missing_desc = conn.execute(f"""
                    SELECT COUNT(*) FROM employment_denormalized 
                    WHERE {code_field} IS NOT NULL AND {desc_field} IS NULL
                """).fetchone()[0]
                
                if missing_desc > 0:
                    logger.warning(f"⚠️ {desc_field} missing for {missing_desc:,} records with {code_field}")
                else:
                    logger.info(f"✅ {desc_field}: complete coverage")
        
        logger.info("\n=== FIELD VALIDATION COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Error during field validation: {e}")

def main():
    """Run full validation."""
    logger.info("Starting validation process...")
    
    # Count raw files
    raw_total, raw_by_dataset = count_raw_files()
    
    # Validate database
    db_total, db_by_dataset = validate_duckdb()
    
    # Compare
    if raw_total > 0 and db_total > 0:
        success = compare_counts(raw_total, raw_by_dataset, db_total, db_by_dataset)
        
        # If counts match, validate field completeness
        if success:
            conn = duckdb.connect('fedscope_employment.duckdb', read_only=True)
            try:
                validate_field_completeness(conn)
                
                # Check for asterisks that could break Hugging Face
                asterisk_check = check_for_asterisks(conn)
                if asterisk_check:
                    logger.info("\n🎉 Validation PASSED - All data loaded correctly and ready for upload!")
                else:
                    logger.error("\n❌ Validation FAILED - Asterisk values found that will break Hugging Face!")
                    success = False
            finally:
                conn.close()
        else:
            logger.warning("\n⚠️ Validation FAILED - Some data may be missing")
    else:
        logger.error("\n❌ Validation could not complete - missing data")

if __name__ == "__main__":
    main()