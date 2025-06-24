"""
FedScope Employment Data Examples - Fixed Version
=================================================

This script demonstrates how to work with the FedScope Employment data using both:
1. Local files (if you've cloned the repository)
2. Direct downloads from GitHub (without cloning)

The examples show common analysis patterns including:
- Loading data
- Counting employees by agency
- Analyzing salaries by education level
- Tracking workforce over time
- Exploring various demographic fields

It also demonstrates how to register multiple Parquet files as external tables in a DuckDB database and query them.
You might want to do this if you want to download multiple data files and query them together.

If you run this, it will ask you if you want to delete the files this downloads. 

"""

import pandas as pd
import os
import shutil
import duckdb
from io import StringIO

def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def safe_int_conversion(value):
    """Safely convert a value to integer, handling various edge cases"""
    if pd.isna(value) or value == 'nan' or value == '*****' or value == '':
        return None
    try:
        return int(float(value))  # float() first handles cases like '123.0'
    except (ValueError, TypeError):
        return None

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
        agency_counts = df.groupby('agysubt')['employment'].apply(
            lambda x: sum(int(i) for i in x if i not in ['nan', '*****', ''])
        ).sort_values(ascending=False).head(10)
        for i, (agency, count) in enumerate(agency_counts.items(), 1):
            print(f"{i:2d}. {agency}: {count:,} employees")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n2. AVERAGE SALARY BY EDUCATION LEVEL (TOP 5)")
    print("-" * 40)
    try:
        # Convert salary to numeric, handling all edge cases
        df['salary_numeric'] = df['salary'].apply(safe_int_conversion)
        
        # Filter to records with valid salary data
        df_with_salary = df[df['salary_numeric'].notna()]
        print(f"   Records with valid salary data: {len(df_with_salary):,} ({len(df_with_salary)/len(df)*100:.1f}%)")
        
        # Calculate average salary by education level
        salary_by_edu = df_with_salary.groupby('edlvlt')['salary_numeric'].mean().sort_values(ascending=False)
        
        for edu, salary in salary_by_edu.head(5).items():
            print(f"   {edu}: ${salary:,.2f}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n3. WORKFORCE BY TIME PERIOD")
    print("-" * 40)
    try:
        quarterly = df.groupby(['year', 'quarter'])['employment'].apply(
            lambda x: sum(int(i) for i in x if i not in ['nan', '*****', ''])
        )
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
    
    print("\n9. SALARY STATISTICS")
    print("-" * 40)
    try:
        if 'salary_numeric' in df.columns:
            valid_salaries = df['salary_numeric'].dropna()
            print(f"   Valid salary records: {len(valid_salaries):,}")
            print(f"   Mean salary: ${valid_salaries.mean():,.2f}")
            print(f"   Median salary: ${valid_salaries.median():,.2f}")
            print(f"   Min salary: ${valid_salaries.min():,.2f}")
            print(f"   Max salary: ${valid_salaries.max():,.2f}")
            print(f"   Std deviation: ${valid_salaries.std():,.2f}")
    except Exception as e:
        print(f"Error: {e}")



def run_duckdb_examples(filenames=None):
    """
    Download FedScope Parquet files, load them into DuckDB, 
    and run an agency head-count query broken out by year.
    
    Args:
        filenames (list): List of parquet filenames to download and analyze.
                         Defaults to September 2024 and 2023 files.
    """
    if filenames is None:
        filenames = [
            "fedscope_employment_September_2024.parquet",
            "fedscope_employment_September_2023.parquet"
        ]
    print("\n" + "="*80)
    print("RUNNING DUCKDB COMBINED-YEAR EXAMPLE")
    print(f"Files to process: {len(filenames)}")
    print("="*80 + "\n")

    # ---------- 1. Make sure download folder exists ----------
    download_dir = "download"
    ensure_directory_exists(download_dir)

    # ---------- 2. Download all requested files ----------
    base_url = ("https://github.com/abigailhaddad/fedscope_employment/"
                "raw/main/fedscope_data/parquet/")

    # Helper to fetch a parquet only if we don’t have it yet
    def fetch_parquet(filename):
        local_path = os.path.join(download_dir, filename)
        if not os.path.exists(local_path):
            print(f"Downloading: {filename}")
            df_tmp = pd.read_parquet(base_url + filename)
            df_tmp.to_parquet(local_path)
            print(f"✓ Saved → {local_path}")
        else:
            print(f"✓ Found cached file → {local_path}")
        return local_path

    # Download all files
    local_files = []
    for filename in filenames:
        local_path = fetch_parquet(filename)
        local_files.append(local_path)

    # ---------- 3. Create / connect to DuckDB and load files ----------
    db_path = os.path.join(download_dir, "fedscope.duckdb")
    con = duckdb.connect(db_path)
    print(f"\n✓ Connected to DuckDB database: {db_path}")
    
    # Create a unified view from all files
    con.execute("DROP VIEW IF EXISTS employment")
    
    # Build UNION ALL query for all files
    union_parts = []
    for local_path in local_files:
        union_parts.append(f"SELECT * FROM read_parquet('{local_path}')")
    
    union_query = " UNION ALL ".join(union_parts)
    view_query = f"CREATE VIEW employment AS {union_query}"
    
    print(f"Creating unified view from {len(local_files)} files...")
    con.execute(view_query)
    print("✓ Created unified employment view")

    # ---------- 4. Run a sample query and pivot wide ----------
    query = """
        SELECT
            year,
            agysubt AS agency_sub,
            SUM(CAST(employment AS INTEGER)) AS employees
        FROM employment
        GROUP BY year, agency_sub
    """

    df_wide = con.execute(query).fetchdf()

    # Pivot to wide format: one row per agency, columns for each year
    df_pivot = df_wide.pivot(index='agency_sub', columns='year', values='employees')

    # Optional: sort by latest year, e.g., 2024
    if 2024 in df_pivot.columns:
        df_pivot = df_pivot.sort_values(by=2024, ascending=False)

    # Display top 10 agencies with highest 2024 headcount (or 2023 if 2024 is missing)
    print("\nTOP AGENCIES BY EMPLOYEES (wide format: one row per agency)\n")
    print(df_pivot.head(10).to_string(index=True, na_rep='–'))


    # ---------- 6. Finish up ----------
    con.close()
    print("\n✓ DuckDB connection closed.")

def cleanup_download_folder():
    """Show download folder contents and ask before removal"""
    download_dir = 'download'
    if os.path.exists(download_dir):
        print(f"\nDownload folder contents:")
        print("-" * 40)
        
        # Show folder contents and sizes
        total_size = 0
        for item in os.listdir(download_dir):
            item_path = os.path.join(download_dir, item)
            if os.path.isfile(item_path):
                size = os.path.getsize(item_path)
                total_size += size
                print(f"  {item} ({size / 1024 / 1024:.1f} MB)")
            else:
                print(f"  {item} (directory)")
        
        print(f"\nTotal size: {total_size / 1024 / 1024:.1f} MB")
        
        # Ask for confirmation
        response = input(f"\nDelete the '{download_dir}' folder and all its contents? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            print(f"Removing {download_dir} folder...")
            shutil.rmtree(download_dir)
            print("✓ Cleanup complete")
        else:
            print(f"✓ Keeping {download_dir} folder")
    else:
        print(f"\nNo {download_dir} folder found to clean up")

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

    # And run DuckDB with local downloads - using default files
    run_duckdb_examples()
    
    # custom_files = ["fedscope_employment_September_2024.parquet", "fedscope_employment_September_2023.parquet"]
    # print(f"\n\nRunning DuckDB example with custom file list: {custom_files}")
    # run_duckdb_examples(custom_files)   
    
    # Cleanup
    cleanup_download_folder()
    
    print("\n" + "="*80)
    print("EXAMPLES COMPLETE")
    print("="*80)
    print("\nFor more information:")
    print("- Repository: https://github.com/abigailhaddad/fedscope_employment")
    print("- Documentation: https://abigailhaddad.github.io/fedscope_employment/")
    print("- Official FedScope: https://www.fedscope.opm.gov/")

def main_with_output_capture():
    """Run main function and capture output to file"""
    output_file = "examples_output.txt"
    
    # Create a custom print function that writes to both console and file
    original_print = print
    original_input = input
    output_buffer = StringIO()
    
    def dual_print(*args, **kwargs):
        # Print to console
        original_print(*args, **kwargs)
        # Also print to buffer (but not file= kwargs)
        if 'file' not in kwargs:
            original_print(*args, **kwargs, file=output_buffer)
    
    def input_with_logging(prompt=""):
        # Print prompt to both console and buffer
        dual_print(prompt, end="")
        # Get actual input from user
        response = original_input()
        # Log the response to buffer
        output_buffer.write(response + "\n")
        return response
    
    # Temporarily replace print and input functions
    import builtins
    builtins.print = dual_print
    builtins.input = input_with_logging
    
    try:
        # Run the main function
        main()
        
        # Write buffer contents to file
        with open(output_file, 'w') as f:
            f.write(output_buffer.getvalue())
        
        original_print(f"\n✓ Output saved to: {output_file}")
        
    finally:
        # Restore original functions
        builtins.print = original_print
        builtins.input = original_input
        output_buffer.close()

if __name__ == "__main__":
    main_with_output_capture()