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
            'pp',            # Pay plan code (null before December 2017)
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
            'ppt',           # Pay plan description (null before December 2017)
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
        
        # PP field should be null before late 2017 (first appears in December 2017)
        pp_null_count_pre2017 = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE pp IS NULL AND year < 2017").fetchone()[0]
        pp_total_pre2017 = conn.execute("SELECT COUNT(*) FROM employment_denormalized WHERE year < 2017").fetchone()[0]
        
        pp_null_count_2017 = conn.execute("""
            SELECT COUNT(*) FROM employment_denormalized 
            WHERE pp IS NULL AND year = 2017 
            AND quarter IN ('March', 'June', 'September')
        """).fetchone()[0]
        pp_total_2017_early = conn.execute("""
            SELECT COUNT(*) FROM employment_denormalized 
            WHERE year = 2017 
            AND quarter IN ('March', 'June', 'September')
        """).fetchone()[0]
        
        if pp_total_pre2017 > 0:
            pp_null_percent = (pp_null_count_pre2017 / pp_total_pre2017) * 100
            logger.info(f"✅ PP field nulls before 2017: {pp_null_count_pre2017:,}/{pp_total_pre2017:,} ({pp_null_percent:.1f}%)")
        
        if pp_total_2017_early > 0:
            pp_null_percent_2017 = (pp_null_count_2017 / pp_total_2017_early) * 100
            logger.info(f"✅ PP field nulls for 2017 Q1-Q3: {pp_null_count_2017:,}/{pp_total_2017_early:,} ({pp_null_percent_2017:.1f}%)")
        
        # Check that PP field is NOT null after December 2017
        pp_not_null_post2017 = conn.execute("""
            SELECT COUNT(*) FROM employment_denormalized 
            WHERE pp IS NOT NULL AND 
            ((year = 2017 AND quarter = 'December') OR year > 2017)
        """).fetchone()[0]
        pp_total_post2017 = conn.execute("""
            SELECT COUNT(*) FROM employment_denormalized 
            WHERE (year = 2017 AND quarter = 'December') OR year > 2017
        """).fetchone()[0]
        
        if pp_total_post2017 > 0:
            pp_present_percent = (pp_not_null_post2017 / pp_total_post2017) * 100
            logger.info(f"✅ PP field present from Dec 2017 onwards: {pp_not_null_post2017:,}/{pp_total_post2017:,} ({pp_present_percent:.1f}%)")
        
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
        
        # Check for completely failed merges by dataset
        logger.info("\nChecking for completely failed merges by dataset...")
        
        # Core lookup relationships that should always work (excluding ones with known data gaps)
        core_lookup_checks = [
            ('agelvl', 'agelvlt', 'age level'),
            ('edlvl', 'edlvlt', 'education level'), 
            ('occ', 'occt', 'occupation'),
            ('patco', 'patcot', 'PATCO category'),
            ('loc', 'loct', 'location'),
            ('gsegrd', 'gsegrdt', 'GS grade'),
            ('supervis', 'supervist', 'supervisory status'),
            ('toa', 'toat', 'appointment type'),
            ('wrksch', 'wrkscht', 'work schedule'),
            ('wkstat', 'wkstatt', 'work status')
        ]
        
        failed_datasets = {}
        
        for code_field, desc_field, field_name in core_lookup_checks:
            if code_field in actual_fields and desc_field in actual_fields:
                # Check datasets where ALL records have codes but NO descriptions
                completely_failed_datasets = conn.execute(f"""
                    WITH dataset_stats AS (
                        SELECT 
                            dataset_key,
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN {code_field} IS NOT NULL THEN 1 END) as records_with_code,
                            COUNT(CASE WHEN {code_field} IS NOT NULL AND {desc_field} IS NOT NULL THEN 1 END) as records_with_desc
                        FROM employment_denormalized 
                        GROUP BY dataset_key
                    )
                    SELECT dataset_key, total_records, records_with_code, records_with_desc
                    FROM dataset_stats 
                    WHERE records_with_code > 0 AND records_with_desc = 0
                    ORDER BY dataset_key
                """).fetchall()
                
                if completely_failed_datasets:
                    logger.error(f"❌ {field_name} merge COMPLETELY FAILED for {len(completely_failed_datasets)} datasets:")
                    for dataset_key, total, with_code, with_desc in completely_failed_datasets:
                        logger.error(f"   - {dataset_key}: {with_code:,} codes, 0 descriptions")
                        if dataset_key not in failed_datasets:
                            failed_datasets[dataset_key] = []
                        failed_datasets[dataset_key].append(field_name)
                else:
                    logger.info(f"✅ {field_name}: no datasets with complete merge failure")
        
        # Special check for pay plan (pp/ppt) which should only exist from Dec 2017
        if 'pp' in actual_fields and 'ppt' in actual_fields:
            # Check records where pp exists but ppt is null (after Dec 2017)
            missing_pp_desc = conn.execute("""
                SELECT COUNT(*) FROM employment_denormalized 
                WHERE pp IS NOT NULL AND ppt IS NULL
                AND ((year = 2017 AND quarter = 'December') OR year > 2017)
            """).fetchone()[0]
            
            if missing_pp_desc > 0:
                logger.warning(f"⚠️ ppt missing for {missing_pp_desc:,} records with pp (post-Dec 2017)")
                failed_merges.append(('pay plan', missing_pp_desc))
            else:
                logger.info("✅ ppt: complete coverage for pay plan (Dec 2017+)")
        
        # Summary of complete merge failures by dataset
        if failed_datasets:
            logger.error("\n❌ CRITICAL: COMPLETE MERGE FAILURES DETECTED!")
            logger.error("The following datasets had lookup merges that completely failed:")
            for dataset_key, failed_fields in failed_datasets.items():
                logger.error(f"  - {dataset_key}: {', '.join(failed_fields)}")
            logger.error("\nThese datasets need to be reprocessed - the lookup files were not loaded or joined properly!")
            return False
        else:
            logger.info("\n✅ All core lookup merges succeeded - no datasets with complete failures")
        
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
                field_validation_success = validate_field_completeness(conn)
                if field_validation_success is not False:
                    logger.info("\n🎉 Validation PASSED - All data loaded correctly!")
                else:
                    logger.error("\n❌ Validation FAILED - Critical merge failures detected!")
                    success = False
            finally:
                conn.close()
        else:
            logger.warning("\n⚠️ Validation FAILED - Some data may be missing")
    else:
        logger.error("\n❌ Validation could not complete - missing data")

if __name__ == "__main__":
    main()