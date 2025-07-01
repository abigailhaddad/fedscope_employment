#!/usr/bin/env python3
"""
Verify that March 2025 and September 2024 data are in the same order
across all aggregated data files.
"""

import pandas as pd
import os

def check_file_order(filename):
    """Check if March and September data are in the same order for a file"""
    print(f"\n=== Checking {filename} ===")
    
    # Read the file
    df = pd.read_csv(filename, sep='\t')
    
    # Split by date code
    march_data = df[df['DATECODE'] == 202503].copy()
    sept_data = df[df['DATECODE'] == 202409].copy()
    
    print(f"March 2025 records: {len(march_data)}")
    print(f"September 2024 records: {len(sept_data)}")
    
    if len(march_data) != len(sept_data):
        print("❌ ERROR: Different number of records!")
        return False
    
    # Get non-date, non-metric columns (the key columns that should match)
    key_columns = [col for col in df.columns 
                   if col not in ['DATECODE', 'EMPCOUNT', 'AVGSAL', 'AVGLOS']]
    
    print(f"Key columns to match: {key_columns}")
    
    # Reset index to compare row by row
    march_data = march_data.reset_index(drop=True)
    sept_data = sept_data.reset_index(drop=True)
    
    # Check if key columns match row by row
    mismatches = 0
    for i in range(len(march_data)):
        march_keys = [str(march_data.loc[i, col]) for col in key_columns]
        sept_keys = [str(sept_data.loc[i, col]) for col in key_columns]
        
        if march_keys != sept_keys:
            if mismatches < 5:  # Only show first 5 mismatches
                print(f"❌ Row {i+1} mismatch:")
                print(f"   March: {march_keys}")
                print(f"   Sept:  {sept_keys}")
            mismatches += 1
    
    if mismatches == 0:
        print("✅ Perfect match! Data is in the same order.")
        
        # Show a few sample comparisons for verification
        print("\nSample comparisons:")
        for i in [0, 1, 2]:
            if i < len(march_data):
                march_count = march_data.loc[i, 'EMPCOUNT']
                sept_count = sept_data.loc[i, 'EMPCOUNT']
                change = march_count - sept_count
                pct_change = (change / sept_count * 100) if sept_count > 0 else 0
                
                # Get first key for identification
                identifier = march_data.loc[i, key_columns[0]] if key_columns else f"Row {i+1}"
                print(f"   {identifier}: {sept_count:,} → {march_count:,} ({change:+,}, {pct_change:+.1f}%)")
        
        return True
    else:
        print(f"❌ ERROR: {mismatches} mismatches found! Data is NOT in the same order.")
        return False

def main():
    """Check all aggregated data files"""
    files = [
        'Status Employment by Age Level_202503_and_202409.txt',
        'Status Employment by Agency and Duty Location_202503_and_202409.txt',
        'Status Employment by Agency and SubAgency_202503_and_202409.txt',
        'Status Employment by Agency_PayPlan_Grade_Series_202503_and_202409.txt',
        'Status Employment by Appointment Type_202503_and_202409.txt',
        'Status Employment by Duty Location_202503_and_202409.txt',
        'Status Employment by Education Level_202503_and_202409.txt',
        'Status Employment by Occupation_202503_and_202409.txt',
        'Status Employment by Pay Plan and Grade_202503_and_202409.txt'
    ]
    
    print("🔍 Verifying data order across all aggregated files...")
    
    all_good = True
    for filename in files:
        if os.path.exists(filename):
            file_ok = check_file_order(filename)
            all_good = all_good and file_ok
        else:
            print(f"\n❌ File not found: {filename}")
            all_good = False
    
    print("\n" + "="*60)
    if all_good:
        print("🎉 SUCCESS: All files have matching data order!")
        print("   It's safe to use index-based pairing in the dashboard.")
    else:
        print("💥 FAILURE: Some files have mismatched data order!")
        print("   The dashboard logic needs to be updated to use key-based matching.")

if __name__ == "__main__":
    main()