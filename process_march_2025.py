#!/usr/bin/env python3
"""
Process March 2025 FedScope data
- Extract ZIP files
- Combine the 3 employment files
- Create parquet file
- Validate row counts
"""

import pandas as pd
import zipfile
import os
import shutil
from pathlib import Path

# Directories
MARCH_DATA_DIR = "fedscope_data/march_2025_data"
EXTRACT_DIR = "fedscope_data/march_2025_extracted"
PARQUET_DIR = "fedscope_data/parquet"

def extract_files():
    """Extract all ZIP files to a temporary directory"""
    print("1. Extracting ZIP files...")
    
    # Create extract directory
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)
    os.makedirs(EXTRACT_DIR)
    
    # Extract each ZIP file
    zip_files = list(Path(MARCH_DATA_DIR).glob("*.zip"))
    print(f"   Found {len(zip_files)} ZIP files")
    
    for zip_file in zip_files:
        print(f"   Extracting {zip_file.name}...")
        with zipfile.ZipFile(zip_file, 'r') as zf:
            zf.extractall(EXTRACT_DIR)
    
    # List extracted files
    txt_files = list(Path(EXTRACT_DIR).glob("March_2025_Employment_*.txt"))
    print(f"   Extracted {len(txt_files)} employment files")
    
    return sorted(txt_files)

def load_and_combine_files(txt_files):
    """Load and combine the 3 employment files"""
    print("\n2. Loading and combining employment files...")
    
    dfs = []
    total_rows = 0
    
    for i, txt_file in enumerate(txt_files, 1):
        print(f"\n   Loading {txt_file.name}...")
        
        # Read with pipe delimiter and handle quotes
        df = pd.read_csv(txt_file, sep='|', quotechar='"', dtype=str)
        
        rows = len(df)
        total_rows += rows
        print(f"      Rows: {rows:,}")
        
        # Check for redacted data
        redacted_count = (df == 'REDACTED').sum().sum()
        redacted_pct = (redacted_count / (rows * len(df.columns))) * 100
        print(f"      Redacted cells: {redacted_count:,} ({redacted_pct:.1f}%)")
        
        # Show sample of columns
        print(f"      Columns: {list(df.columns[:5])}... ({len(df.columns)} total)")
        
        # Check unique agencies
        if 'AGYT' in df.columns:
            n_agencies = df['AGYT'].nunique()
            print(f"      Unique agencies: {n_agencies}")
        
        dfs.append(df)
    
    print(f"\n   Total rows across all files: {total_rows:,}")
    
    # Combine all dataframes
    print("\n   Combining dataframes...")
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"   Combined rows: {len(combined_df):,}")
    
    return combined_df

def standardize_columns(df):
    """Transform columns to match historical format exactly"""
    print("\n3. Standardizing columns to match historical format...")
    
    # Load September 2024 to get exact column structure
    sept_2024 = pd.read_parquet('fedscope_data/parquet/fedscope_employment_September_2024.parquet')
    target_columns = list(sept_2024.columns)
    print(f"   Target format has {len(target_columns)} columns")
    
    # Create new dataframe with historical structure
    new_df = pd.DataFrame()
    
    # Map the available columns
    column_mapping = {
        # Direct mappings from March 2025 to historical format
        'AGY': 'agy',
        'AGYSUB': 'agysub', 
        'LOC': 'loc',
        'AGELVLT': 'agelvlt',
        'EDLVL': 'edlvl',
        'EDLVLT': 'edlvlt',
        'LOS': 'los',
        'OCC': 'occ',
        'OCCFAM': 'occfam',
        'OCCFAMT': 'occfamt',
        'OCCT': 'occt',
        'PAYPLAN': 'payplan',
        'PAYPLANT': 'payplant',
        'SUPERVIS': 'supervis',
        'SUPERVIST': 'supervist',
        'TOA': 'toa',
        'TOAT': 'toat',
        'WORKSCH': 'worksch',
        'WORKSCHT': 'wrkscht',
        'DATECODE': 'datecode',
        'COUNT': 'employment',
        'SALARY': 'salary',
        'AGYSUBT': 'agysubt',
        'STATET': 'loct'  # State name maps to location text
    }
    
    # Copy directly mapped columns
    for march_col, hist_col in column_mapping.items():
        if march_col in df.columns and hist_col in target_columns:
            new_df[hist_col] = df[march_col]
    
    # Set standard values for missing historical columns
    new_df['dataset_key'] = 'March_2025'
    new_df['quarter'] = 'March'
    new_df['year'] = 2025
    
    # Fill in missing required columns with appropriate defaults/derived values
    missing_cols = set(target_columns) - set(new_df.columns)
    print(f"   Filling {len(missing_cols)} missing columns with defaults")
    
    for col in missing_cols:
        if col in ['gsegrd', 'loslvl', 'patco', 'pp', 'ppgrd', 'sallvl', 'stemocc', 'workstat']:
            # These might need to be derived or set to defaults
            new_df[col] = ''  # Empty string for missing categorical data
        elif col in ['loslvlt', 'occtyp', 'occtypt', 'patcot', 'pptyp', 'ppgrdt', 'ppgroup', 'ppgroupt', 'sallvlt', 'stemocct', 'wstyp', 'wstypt', 'wkstatt', 'ppt', 'wrksch', 'wkstat']:
            # These are text descriptions that we don't have
            new_df[col] = ''
        else:
            new_df[col] = ''
    
    # Reorder columns to match historical format exactly
    new_df = new_df[target_columns]
    
    print(f"   Final dataframe has {len(new_df.columns)} columns matching historical format")
    print(f"   Sample columns: {list(new_df.columns[:10])}...")
    
    return new_df

def validate_data(df):
    """Validate the data looks reasonable"""
    print("\n4. Validating data...")
    
    # Check row count
    row_count = len(df)
    print(f"\n   Total employees: {row_count:,}")
    
    if row_count < 1_500_000 or row_count > 2_500_000:
        print("   ⚠️  WARNING: Row count outside expected range (1.5M - 2.5M)")
    else:
        print("   ✓ Row count looks reasonable")
    
    # Check agencies
    if 'agyt' in df.columns:
        top_agencies = df['agyt'].value_counts().head(10)
        print("\n   Top 10 agencies by employee count:")
        for agency, count in top_agencies.items():
            if agency != 'REDACTED':
                print(f"      {agency}: {count:,}")
    
    # Check salary data
    if 'salary' in df.columns:
        non_redacted_salaries = df[df['salary'] != 'REDACTED']['salary'].astype(float)
        if len(non_redacted_salaries) > 0:
            print(f"\n   Salary statistics (non-redacted):")
            print(f"      Mean: ${non_redacted_salaries.mean():,.0f}")
            print(f"      Median: ${non_redacted_salaries.median():,.0f}")
    
    # Check redaction levels
    redacted_counts = (df == 'REDACTED').sum()
    high_redaction_cols = redacted_counts[redacted_counts > row_count * 0.9]
    if len(high_redaction_cols) > 0:
        print(f"\n   Highly redacted columns (>90%):")
        for col in high_redaction_cols.index:
            pct = (redacted_counts[col] / row_count) * 100
            print(f"      {col}: {pct:.1f}%")

def save_parquet(df):
    """Save to parquet format"""
    print("\n5. Saving to parquet...")
    
    # Create parquet directory if needed
    os.makedirs(PARQUET_DIR, exist_ok=True)
    
    # Save file
    output_file = os.path.join(PARQUET_DIR, "fedscope_employment_March_2025.parquet")
    df.to_parquet(output_file, index=False)
    
    # Check file size
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
    print(f"   Saved to: {output_file}")
    print(f"   File size: {file_size:.1f} MB")
    
    # Verify it can be read back
    test_df = pd.read_parquet(output_file)
    print(f"   Verified: Successfully read back {len(test_df):,} rows")

def main():
    """Main processing pipeline"""
    print("Processing March 2025 FedScope Employment Data")
    print("=" * 60)
    
    # Extract files
    txt_files = extract_files()
    
    # Load and combine
    df = load_and_combine_files(txt_files)
    
    # Standardize columns
    df = standardize_columns(df)
    
    # Validate
    validate_data(df)
    
    # Save
    save_parquet(df)
    
    # Move documentation
    print("\n6. Moving documentation...")
    pdf_source = os.path.join(EXTRACT_DIR, "FedScope Employment.pdf")
    if os.path.exists(pdf_source):
        os.makedirs("documentation_pdfs", exist_ok=True)
        pdf_dest = "documentation_pdfs/FedScope_Employment_March_2025.pdf"
        shutil.move(pdf_source, pdf_dest)
        print(f"   Moved documentation to: {pdf_dest}")
    else:
        print("   No PDF documentation found")
    
    # Cleanup
    print("\n7. Cleaning up...")
    shutil.rmtree(EXTRACT_DIR)
    print("   Removed temporary extraction directory")
    
    print("\n✅ Processing complete!")

if __name__ == "__main__":
    main()