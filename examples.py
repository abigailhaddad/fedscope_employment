"""
FedScope Employment Data Examples
=================================

This script demonstrates how to work with the FedScope Employment data using both:
1. Local files (if you've cloned the repository)
2. Direct downloads from GitHub (without cloning)

The examples show common analysis patterns including:
- Loading data
- Counting employees by agency
- Analyzing salaries by education level
- Tracking workforce over time
- Exploring various demographic fields

Note: The data uses string types for numeric fields like 'employment' and 'salary'.
This script handles the conversions appropriately.
"""

import pandas as pd
import os
import shutil

def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def run_local_examples():
    """Run examples using local parquet files"""
    print("\n" + "="*80)
    print("RUNNING EXAMPLES WITH LOCAL FILES")
    print("="*80 + "\n")
    
    # Check if local files exist
    local_file = 'fedscope_data/parquet/fedscope_employment_September_2024.parquet'
    
    if not os.path.exists(local_file):
        print(f"ERROR: Local file not found: {local_file}")
        print("\nThis script expects you to have cloned the full repository.")
        print("If you haven't cloned the repo, the download examples below will work instead.")
        print("\nTo use local files:")
        print("1. Clone the repository: git clone https://github.com/abigailhaddad/fedscope_employment.git")
        print("2. Run this script from the repository root directory")
        return None
    
    print(f"Loading local file: {local_file}")
    df = pd.read_parquet(local_file)
    print(f"✓ Successfully loaded {len(df):,} records from September 2024")
    print(f"  Columns: {df.shape[1]}")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    return df

def run_download_examples():
    """Run examples by downloading files from GitHub"""
    print("\n" + "="*80)
    print("RUNNING EXAMPLES WITH GITHUB DOWNLOADS")
    print("="*80 + "\n")
    
    # Create download directory
    download_dir = 'download'
    ensure_directory_exists(download_dir)
    
    # Download URL
    url = 'https://github.com/abigailhaddad/fedscope_employment/raw/main/fedscope_data/parquet/fedscope_employment_September_2024.parquet'
    local_download_path = os.path.join(download_dir, 'fedscope_employment_September_2024.parquet')
    
    print(f"Downloading from: {url}")
    print(f"Saving to: {local_download_path}")
    print("This may take a moment (file is ~30-40 MB)...")
    
    try:
        df = pd.read_parquet(url)
        # Save locally for faster subsequent access in this session
        df.to_parquet(local_download_path)
        print(f"✓ Successfully downloaded and loaded {len(df):,} records")
        print(f"  Also saved locally to: {local_download_path}")
        return df
    except Exception as e:
        print(f"ERROR downloading file: {e}")
        print("\nPossible causes:")
        print("- No internet connection")
        print("- GitHub rate limiting")
        print("- File URL has changed")
        return None

def analyze_data(df, source_type):
    """Run analysis examples on the dataframe"""
    print(f"\n{'='*80}")
    print(f"ANALYSIS EXAMPLES ({source_type})")
    print(f"{'='*80}\n")
    
    # Example 1: Count employees by agency
    print("1. TOP 10 AGENCIES BY EMPLOYEE COUNT")
    print("-" * 40)
    try:
        # Employment is stored as string '1' for each record
        agency_counts = df.groupby('agysubt')['employment'].apply(lambda x: sum(int(i) for i in x)).sort_values(ascending=False).head(10)
        for i, (agency, count) in enumerate(agency_counts.items(), 1):
            print(f"{i:2d}. {agency}: {count:,} employees")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n2. AVERAGE SALARY BY EDUCATION LEVEL (TOP 5)")
    print("-" * 40)
    try:
        # Filter out null salaries and asterisks (redacted values)
        df_with_salary = df[df['salary'].notna() & (df['salary'] != '*****')]
        print(f"   Records with valid salary data: {len(df_with_salary):,} ({len(df_with_salary)/len(df)*100:.1f}%)")
        
        # Calculate average salary by education level
        salary_by_edu = df_with_salary.groupby('edlvlt')['salary'].apply(
            lambda x: sum(int(i) for i in x) / len(x)
        ).sort_values(ascending=False)
        
        for edu, salary in salary_by_edu.head(5).items():
            print(f"   {edu}: ${salary:,.2f}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n3. WORKFORCE BY TIME PERIOD")
    print("-" * 40)
    try:
        quarterly = df.groupby(['year', 'quarter'])['employment'].apply(lambda x: sum(int(i) for i in x))
        for (year, quarter), count in quarterly.items():
            print(f"   {year} {quarter}: {count:,} employees")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n4. LOCATION DISTRIBUTION (TOP 5 STATES)")
    print("-" * 40)
    try:
        top_locations = df['loct'].value_counts().head(5)
        for location, count in top_locations.items():
            print(f"   {location}: {count:,} employees ({count/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n5. JOB CATEGORIES (PATCO)")
    print("-" * 40)
    try:
        patco_dist = df['patcot'].value_counts()
        for category, count in patco_dist.items():
            print(f"   {category}: {count:,} employees ({count/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n6. AGE DISTRIBUTION")
    print("-" * 40)
    try:
        age_dist = df['agelvlt'].value_counts().sort_index()
        for age_group, count in age_dist.items():
            print(f"   {age_group}: {count:,} employees ({count/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n7. SUPERVISORY STATUS")
    print("-" * 40)
    try:
        super_dist = df['supervist'].value_counts()
        for status, count in super_dist.items():
            print(f"   {status}: {count:,} employees ({count/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n8. WORK SCHEDULE")
    print("-" * 40)
    try:
        work_schedule = df['wrkscht'].value_counts()
        for schedule, count in work_schedule.items():
            print(f"   {schedule}: {count:,} employees ({count/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")

def cleanup_download_folder():
    """Remove the download folder if it exists"""
    download_dir = 'download'
    if os.path.exists(download_dir):
        print(f"\nCleaning up: Removing {download_dir} folder...")
        shutil.rmtree(download_dir)
        print("✓ Cleanup complete")

def main():
    """Main function to run all examples"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║           FedScope Employment Data - Usage Examples              ║
║                                                                  ║
║  This script demonstrates working with 140+ million federal     ║
║  employee records from 1998-2024.                               ║
║                                                                  ║
║  Data source: https://github.com/abigailhaddad/fedscope_employment
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Try local files first
    df_local = run_local_examples()
    if df_local is not None:
        analyze_data(df_local, "LOCAL FILES")
    
    # Then demonstrate download method
    df_download = run_download_examples()
    if df_download is not None:
        analyze_data(df_download, "DOWNLOADED FILES")
    
    # Cleanup
    cleanup_download_folder()
    
    print("\n" + "="*80)
    print("EXAMPLES COMPLETE")
    print("="*80)
    print("\nFor more information:")
    print("- Repository: https://github.com/abigailhaddad/fedscope_employment")
    print("- Documentation: https://abigailhaddad.github.io/fedscope_employment/")
    print("- Official FedScope: https://www.fedscope.opm.gov/")

if __name__ == "__main__":
    main()