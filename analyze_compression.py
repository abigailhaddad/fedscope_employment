#!/usr/bin/env python3
"""
Script to analyze data compression by grouping all variables and aggregating by count.
Tests how much data compression we can achieve through aggregation.
"""

import os
import zipfile
import pandas as pd
import numpy as np
import random
import glob
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_file_size_mb(file_path):
    """Get file size in MB."""
    return os.path.getsize(file_path) / (1024 * 1024)

def select_random_zip():
    """Select a random zip file from the fedscope_data directory."""
    fedscope_dir = "fedscope_data"
    zip_files = glob.glob(os.path.join(fedscope_dir, "*.zip"))
    if not zip_files:
        raise FileNotFoundError("No zip files found in fedscope_data directory")
    
    selected_zip = random.choice(zip_files)
    logger.info(f"Selected random zip file: {os.path.basename(selected_zip)}")
    return selected_zip

def extract_zip_to_temp(zip_path, temp_dir="temp_extraction"):
    """Extract zip file to temporary directory."""
    if os.path.exists(temp_dir):
        import shutil
        shutil.rmtree(temp_dir)
    
    os.makedirs(temp_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(temp_dir)
    
    logger.info(f"Extracted {os.path.basename(zip_path)} to {temp_dir}")
    return temp_dir

def find_factdata_file(extracted_dir):
    """Find the FACTDATA file in the extracted directory."""
    factdata_files = glob.glob(os.path.join(extracted_dir, "**/FACTDATA_*.TXT"), recursive=True)
    if not factdata_files:
        raise FileNotFoundError("No FACTDATA file found in extracted directory")
    
    factdata_file = factdata_files[0]
    logger.info(f"Found FACTDATA file: {os.path.basename(factdata_file)}")
    return factdata_file

def analyze_compression_potential(factdata_file):
    """Analyze compression potential by aggregating data."""
    logger.info("Starting compression analysis...")
    
    # Measure original file size
    original_size_mb = get_file_size_mb(factdata_file)
    logger.info(f"Original FACTDATA file size: {original_size_mb:.2f} MB")
    
    # Load the data
    logger.info("Loading data into pandas...")
    df = pd.read_csv(factdata_file, dtype=str)
    logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Calculate memory usage of original data
    original_memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    logger.info(f"Original data memory usage: {original_memory_mb:.2f} MB")
    
    # Find all non-numeric columns (these will be our grouping variables)
    grouping_cols = []
    numeric_cols = []
    
    for col in df.columns:
        # Try to convert to numeric to see if it's a count/value column
        numeric_vals = pd.to_numeric(df[col], errors='coerce')
        if numeric_vals.notna().sum() > len(df) * 0.8:  # If 80%+ are numeric
            numeric_cols.append(col)
        else:
            grouping_cols.append(col)
    
    logger.info(f"Identified {len(grouping_cols)} grouping columns: {grouping_cols}")
    logger.info(f"Identified {len(numeric_cols)} numeric columns: {numeric_cols}")
    
    # If no clear numeric columns, treat all as grouping and count occurrences
    if not numeric_cols:
        logger.info("No clear numeric columns found, will count occurrences of unique combinations")
        
        # Group by all columns and count occurrences
        logger.info("Grouping by all columns and counting occurrences...")
        aggregated_df = df.groupby(list(df.columns)).size().reset_index(name='count')
        
    else:
        # Group by non-numeric columns and sum numeric columns
        logger.info("Grouping by categorical columns and summing numeric columns...")
        
        # Convert numeric columns to actual numeric types
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Group and aggregate
        agg_dict = {col: 'sum' for col in numeric_cols}
        aggregated_df = df.groupby(grouping_cols).agg(agg_dict).reset_index()
    
    logger.info(f"Aggregated to {len(aggregated_df):,} unique combinations")
    
    # Calculate memory usage of aggregated data
    aggregated_memory_mb = aggregated_df.memory_usage(deep=True).sum() / (1024 * 1024)
    logger.info(f"Aggregated data memory usage: {aggregated_memory_mb:.2f} MB")
    
    # Save aggregated data to see file size
    temp_csv = "temp_aggregated.csv"
    aggregated_df.to_csv(temp_csv, index=False)
    aggregated_size_mb = get_file_size_mb(temp_csv)
    
    # Calculate compression ratios
    memory_compression_ratio = (original_memory_mb - aggregated_memory_mb) / original_memory_mb * 100
    file_compression_ratio = (original_size_mb - aggregated_size_mb) / original_size_mb * 100
    
    # Print results
    print("\n" + "="*60)
    print("COMPRESSION ANALYSIS RESULTS")
    print("="*60)
    print(f"Original file size:           {original_size_mb:.2f} MB")
    print(f"Aggregated file size:         {aggregated_size_mb:.2f} MB")
    print(f"File size reduction:          {file_compression_ratio:.1f}%")
    print()
    print(f"Original memory usage:        {original_memory_mb:.2f} MB")
    print(f"Aggregated memory usage:      {aggregated_memory_mb:.2f} MB")
    print(f"Memory usage reduction:       {memory_compression_ratio:.1f}%")
    print()
    print(f"Original row count:           {len(df):,}")
    print(f"Aggregated row count:         {len(aggregated_df):,}")
    print(f"Row count reduction:          {(len(df) - len(aggregated_df)) / len(df) * 100:.1f}%")
    print("="*60)
    
    # Clean up
    if os.path.exists(temp_csv):
        os.remove(temp_csv)
    
    return {
        'original_size_mb': original_size_mb,
        'aggregated_size_mb': aggregated_size_mb,
        'file_compression_ratio': file_compression_ratio,
        'original_memory_mb': original_memory_mb,
        'aggregated_memory_mb': aggregated_memory_mb,
        'memory_compression_ratio': memory_compression_ratio,
        'original_rows': len(df),
        'aggregated_rows': len(aggregated_df),
        'row_reduction_ratio': (len(df) - len(aggregated_df)) / len(df) * 100
    }

def main():
    """Main function to run compression analysis."""
    try:
        # Select random zip file
        zip_file = select_random_zip()
        
        # Extract it
        temp_dir = extract_zip_to_temp(zip_file)
        
        # Find FACTDATA file
        factdata_file = find_factdata_file(temp_dir)
        
        # Analyze compression potential
        results = analyze_compression_potential(factdata_file)
        
        # Clean up
        import shutil
        shutil.rmtree(temp_dir)
        
        logger.info("Analysis complete!")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()