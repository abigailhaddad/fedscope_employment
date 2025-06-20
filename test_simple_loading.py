#!/usr/bin/env python3
"""
Simple test to load FedScope data as CSV directly.
"""

import pandas as pd

# Direct CSV download URL pattern for Hugging Face datasets
base_url = "https://huggingface.co/datasets/abigailhaddad/fedscope/resolve/main"
file_name = "fedscope_employment_September_2022.csv"

print(f"Attempting to load {file_name} directly...")
try:
    # Try loading directly from URL
    url = f"{base_url}/{file_name}"
    df = pd.read_csv(url)
    
    print(f"✅ Successfully loaded CSV!")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))
    
    print(f"\nColumn names:")
    print(list(df.columns)[:10], "...")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nThis might mean:")
    print("- The file hasn't been uploaded yet")
    print("- The file name is different")
    print("- The dataset is private")