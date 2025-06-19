#!/usr/bin/env python3
"""
Verification script to ensure FedScope data is properly loaded in PostgreSQL.
Checks data integrity, completeness, and provides summary statistics.
"""

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_table_exists(cur, table_name):
    """Check if a table exists in PostgreSQL."""
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def get_table_count(cur, table_name):
    """Get row count for a table."""
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting count for {table_name}: {e}")
        return 0

def verify_data_integrity(cur):
    """Perform data integrity checks."""
    logger.info("Performing data integrity checks...")
    
    issues = []
    
    # Check for duplicate employment facts
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM fedscope_employment_facts")
    total_count, unique_count = cur.fetchone()
    if total_count != unique_count:
        issues.append(f"Employment facts has duplicate IDs: {total_count} total vs {unique_count} unique")
    
    # Check for missing dataset keys in employment facts
    cur.execute("SELECT COUNT(*) FROM fedscope_employment_facts WHERE dataset_key IS NULL OR dataset_key = ''")
    null_datasets = cur.fetchone()[0]
    if null_datasets > 0:
        issues.append(f"Employment facts has {null_datasets} records with missing dataset_key")
    
    # Check for invalid employment values
    cur.execute("SELECT COUNT(*) FROM fedscope_employment_facts WHERE employment < 0")
    negative_employment = cur.fetchone()[0]
    if negative_employment > 0:
        issues.append(f"Employment facts has {negative_employment} records with negative employment")
    
    # Check for unrealistic salary values
    cur.execute("SELECT COUNT(*) FROM fedscope_employment_facts WHERE salary > 1000000 OR salary < 0")
    unrealistic_salary = cur.fetchone()[0]
    if unrealistic_salary > 0:
        issues.append(f"Employment facts has {unrealistic_salary} records with unrealistic salary values")
    
    if issues:
        logger.warning("Data integrity issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("✓ No data integrity issues found")
    
    return len(issues) == 0

def get_dataset_summary(cur):
    """Get summary of datasets loaded."""
    logger.info("Getting dataset summary...")
    
    # Get unique datasets from employment facts
    cur.execute("""
        SELECT 
            dataset_key,
            year,
            quarter,
            COUNT(*) as record_count,
            SUM(employment) as total_employment,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM fedscope_employment_facts 
        WHERE salary IS NOT NULL
        GROUP BY dataset_key, year, quarter
        ORDER BY year, quarter
    """)
    
    results = cur.fetchall()
    
    logger.info("\nDataset Summary (Employment Facts):")
    logger.info("Dataset Key    | Records    | Employment | Avg Salary | Salary Range")
    logger.info("-" * 70)
    
    total_records = 0
    total_employment = 0
    
    for row in results:
        dataset_key, year, quarter, record_count, employment, avg_sal, min_sal, max_sal = row
        total_records += record_count
        total_employment += employment if employment else 0
        
        avg_sal_str = f"${avg_sal:,.0f}" if avg_sal else "N/A"
        salary_range = f"${min_sal:,.0f}-${max_sal:,.0f}" if min_sal and max_sal else "N/A"
        
        logger.info(f"{dataset_key:12} | {record_count:9,} | {employment:9,} | {avg_sal_str:9} | {salary_range}")
    
    logger.info("-" * 70)
    logger.info(f"TOTAL:       | {total_records:9,} | {total_employment:9,}")
    
    return results

def get_lookup_summary(cur):
    """Get summary of lookup tables."""
    logger.info("\nGetting lookup table summary...")
    
    lookup_tables = [
        'fedscope_lookup_agelvl',
        'fedscope_lookup_agency', 
        'fedscope_lookup_date',
        'fedscope_lookup_education',
        'fedscope_lookup_gsegrd',
        'fedscope_lookup_location',
        'fedscope_lookup_loslvl',
        'fedscope_lookup_occupation',
        'fedscope_lookup_patco',
        'fedscope_lookup_payplan',
        'fedscope_lookup_ppgrd',
        'fedscope_lookup_salary_level',
        'fedscope_lookup_stemocc',
        'fedscope_lookup_supervisory',
        'fedscope_lookup_appointment',
        'fedscope_lookup_work_schedule',
        'fedscope_lookup_work_status'
    ]
    
    logger.info("\nLookup Table Summary:")
    logger.info("Table Name                     | Record Count | Unique Datasets")
    logger.info("-" * 60)
    
    total_lookup_records = 0
    
    for table in lookup_tables:
        if verify_table_exists(cur, table):
            count = get_table_count(cur, table)
            
            # Get unique dataset count
            cur.execute(f"SELECT COUNT(DISTINCT dataset_key) FROM {table}")
            unique_datasets = cur.fetchone()[0]
            
            total_lookup_records += count
            table_short = table.replace('fedscope_lookup_', '')
            logger.info(f"{table_short:30} | {count:11,} | {unique_datasets:14}")
        else:
            logger.warning(f"{table:30} | NOT FOUND")
    
    logger.info("-" * 60)
    logger.info(f"TOTAL LOOKUP RECORDS:          | {total_lookup_records:11,}")

def check_data_completeness(cur):
    """Check data completeness across years and quarters."""
    logger.info("\nChecking data completeness...")
    
    # Get expected datasets based on file structure
    cur.execute("""
        SELECT DISTINCT year, quarter 
        FROM fedscope_employment_facts 
        ORDER BY year, quarter
    """)
    
    loaded_datasets = cur.fetchall()
    
    logger.info("Loaded datasets by year/quarter:")
    year_counts = {}
    
    for year, quarter in loaded_datasets:
        if year not in year_counts:
            year_counts[year] = []
        year_counts[year].append(quarter)
    
    for year in sorted(year_counts.keys()):
        quarters = sorted(year_counts[year])
        logger.info(f"  {year}: {', '.join(quarters)} ({len(quarters)} quarters)")
    
    # Check for potential gaps
    logger.info("\nData completeness analysis:")
    
    total_years = len(year_counts)
    total_quarters = sum(len(quarters) for quarters in year_counts.values())
    
    logger.info(f"  Total years with data: {total_years}")
    logger.info(f"  Total quarter datasets: {total_quarters}")
    
    # Look for years with missing quarters
    incomplete_years = []
    for year, quarters in year_counts.items():
        if len(quarters) < 4:  # Expecting 4 quarters per year typically
            incomplete_years.append((year, quarters))
    
    if incomplete_years:
        logger.warning("Years with potentially incomplete quarterly data:")
        for year, quarters in incomplete_years:
            missing = set(['Q1', 'Q2', 'Q3', 'Q4']) - set(quarters)
            if missing:
                logger.warning(f"  {year}: Missing {', '.join(sorted(missing))}")
    else:
        logger.info("✓ All years appear to have complete quarterly data")

def run_sample_queries(cur):
    """Run sample analytical queries to verify data usability."""
    logger.info("\nRunning sample analytical queries...")
    
    # Query 1: Top agencies by employment
    logger.info("Top 10 agencies by total employment (latest quarter):")
    cur.execute("""
        SELECT 
            a.agysub_desc,
            SUM(f.employment) as total_employment
        FROM fedscope_employment_facts f
        JOIN fedscope_lookup_agency a ON f.dataset_key = a.dataset_key
        WHERE f.dataset_key = (SELECT MAX(dataset_key) FROM fedscope_employment_facts)
        GROUP BY a.agysub_desc
        ORDER BY total_employment DESC
        LIMIT 10
    """)
    
    results = cur.fetchall()
    for i, (agency, employment) in enumerate(results, 1):
        logger.info(f"  {i:2}. {agency[:50]:50} | {employment:8,}")
    
    # Query 2: Average salary by education level
    logger.info("\nAverage salary by education level (latest quarter):")
    cur.execute("""
        SELECT 
            e.edlvl_desc,
            AVG(f.salary) as avg_salary,
            COUNT(*) as record_count
        FROM fedscope_employment_facts f
        JOIN fedscope_lookup_education e ON f.dataset_key = e.dataset_key
        WHERE f.dataset_key = (SELECT MAX(dataset_key) FROM fedscope_employment_facts)
        AND f.salary IS NOT NULL
        GROUP BY e.edlvl_desc
        HAVING COUNT(*) > 1000
        ORDER BY avg_salary DESC
        LIMIT 10
    """)
    
    results = cur.fetchall()
    for edu_level, avg_sal, count in results:
        logger.info(f"  {edu_level[:40]:40} | ${avg_sal:8,.0f} | {count:6,} records")

def main():
    """Main verification function."""
    logger.info("="*60)
    logger.info("FEDSCOPE POSTGRESQL DATA VERIFICATION")
    logger.info("="*60)
    
    # Connect to PostgreSQL
    conn_str = os.getenv('DATABASE_URL')
    if not conn_str:
        logger.error("DATABASE_URL environment variable not set")
        logger.error("Please set DATABASE_URL in your .env file")
        return False
    
    try:
        pg_conn = psycopg2.connect(conn_str)
        cur = pg_conn.cursor()
        logger.info("✓ Connected to PostgreSQL")
        
        # Check that main tables exist
        required_tables = [
            'fedscope_employment_facts',
            'fedscope_lookup_agency',
            'fedscope_lookup_occupation',
            'fedscope_lookup_location'
        ]
        
        missing_tables = []
        for table in required_tables:
            if not verify_table_exists(cur, table):
                missing_tables.append(table)
        
        if missing_tables:
            logger.error("Missing required tables:")
            for table in missing_tables:
                logger.error(f"  - {table}")
            return False
        
        logger.info("✓ All required tables exist")
        
        # Get basic table counts
        logger.info("\nBasic Table Verification:")
        facts_count = get_table_count(cur, 'fedscope_employment_facts')
        logger.info(f"  Employment Facts: {facts_count:,} records")
        
        if facts_count == 0:
            logger.error("No employment facts data found!")
            return False
        
        # Run comprehensive checks
        integrity_ok = verify_data_integrity(cur)
        get_dataset_summary(cur)
        get_lookup_summary(cur)
        check_data_completeness(cur)
        run_sample_queries(cur)
        
        # Final verdict
        logger.info("\n" + "="*60)
        if integrity_ok and facts_count > 0:
            logger.info("✓ VERIFICATION PASSED - Data appears to be loaded correctly")
            logger.info("PostgreSQL database is ready for analysis!")
        else:
            logger.warning("⚠ VERIFICATION ISSUES - Please review the issues above")
        logger.info("="*60)
        
        return integrity_ok and facts_count > 0
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")
        return False
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()

if __name__ == "__main__":
    main()