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
                quarter_map = {
                    'March': 'Q1', 'June': 'Q2', 'September': 'Q3', 'December': 'Q4'
                }
                quarter = quarter_map.get(month, month)
                dataset_key = f"{year}_{quarter}"
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
        if success:
            logger.info("\n🎉 Validation PASSED - All data loaded correctly!")
        else:
            logger.warning("\n⚠️ Validation FAILED - Some data may be missing")
    else:
        logger.error("\n❌ Validation could not complete - missing data")

if __name__ == "__main__":
    main()