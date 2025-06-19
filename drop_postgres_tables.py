#!/usr/bin/env python3
"""
Drop all FedScope tables from PostgreSQL database.
"""

import psycopg2
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def drop_all_fedscope_tables():
    """Drop all FedScope tables from PostgreSQL."""
    
    # Connect to PostgreSQL
    conn_str = os.getenv('DATABASE_URL')
    if not conn_str:
        logger.error("DATABASE_URL environment variable not set")
        logger.error("Please set DATABASE_URL in your .env file")
        return False
    
    try:
        pg_conn = psycopg2.connect(conn_str)
        cur = pg_conn.cursor()
        logger.info("Connected to PostgreSQL")
        
        # List all FedScope tables to drop
        tables_to_drop = [
            'fedscope_employment_facts',
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
        
        logger.info(f"Dropping {len(tables_to_drop)} FedScope tables...")
        
        dropped_count = 0
        for table in tables_to_drop:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                logger.info(f"  ✓ Dropped: {table}")
                dropped_count += 1
            except Exception as e:
                logger.error(f"  ✗ Could not drop {table}: {e}")
        
        pg_conn.commit()
        cur.close()
        pg_conn.close()
        
        logger.info(f"\nSuccessfully dropped {dropped_count}/{len(tables_to_drop)} tables")
        logger.info("Database is now clean and ready for fresh data load")
        
        return True
        
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    logger.info("="*50)
    logger.info("DROPPING FEDSCOPE TABLES FROM POSTGRESQL")
    logger.info("="*50)
    
    success = drop_all_fedscope_tables()
    
    if success:
        logger.info("✓ Tables dropped successfully")
    else:
        logger.error("✗ Failed to drop tables")