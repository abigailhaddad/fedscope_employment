#!/usr/bin/env python3
"""
Test script to verify that the Hugging Face dataset can be loaded properly.
"""

from datasets import load_dataset
import pandas as pd

def test_single_file_loading():
    """Test loading a single CSV file from the dataset."""
    print("Testing FedScope dataset loading from Hugging Face...")
    print("-" * 50)
    
    try:
        # Load just September 2022
        print("Loading September 2022 data...")
        ds = load_dataset(
            "abigailhaddad/fedscope", 
            data_files="fedscope_employment_September_2022.csv"
        )
        
        # If it returns a DatasetDict, get the train split
        if hasattr(ds, 'keys'):
            ds = ds['train']
        
        print(f"✅ Successfully loaded dataset!")
        print(f"   Number of records: {len(ds):,}")
        print(f"   Features: {list(ds.features.keys())[:10]}... ({len(ds.features)} total)")
        
        # Convert first few rows to pandas to inspect
        print("\nFirst 3 rows:")
        df = ds.to_pandas().head(3)
        print(df)
        
        # Check for nulls in key fields
        print(f"\nChecking data quality:")
        df_full = ds.to_pandas()
        print(f"   Salary nulls: {df_full['salary'].isna().sum():,}")
        print(f"   Education nulls: {df_full['edlvl'].isna().sum():,}")
        
        print("\n✅ Dataset loads successfully and can be used for analysis!")
        
    except Exception as e:
        print(f"❌ Error loading dataset: {e}")
        print("\nMake sure the dataset has been uploaded to Hugging Face first.")

if __name__ == "__main__":
    test_single_file_loading()