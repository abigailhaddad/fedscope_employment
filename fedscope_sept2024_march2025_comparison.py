"""
FedScope Employment Data Comparison: September 2024 vs March 2025
================================================================

⚠️ REDACTION ISSUES IN MARCH 2025 DATA ⚠️

March 2025 has significant redaction that makes direct comparisons problematic.
This script demonstrates:

1. How to analyze redaction patterns
2. How to work around redaction by excluding affected agencies

Key Functions for Working Around Redaction:
- get_agencies_with_redacted_data(df, field): Identify agencies with redacted data
- filter_out_redacted_agencies(df_march, df_sept, agencies): Exclude agencies from both datasets
- Example: Age comparison excluding agencies AF, AR, DD, NV (have redacted age data)

Important: Category names changed between time periods, so use coded fields (edlvl, toa, agy) 
for accurate matching, then map to descriptive names from both datasets.

Data Sources:
- March 2025: fedscope_employment_March_2025.parquet 
- September 2024: fedscope_employment_September_2024.parquet
"""

import pandas as pd
import numpy as np
from great_tables import GT, html, md, style, loc
from typing import Dict, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CELL 1: Setup and Data Loading
# =============================================================================

def load_fedscope_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load March 2025 and September 2024 FedScope data from GitHub.
    
    Returns:
        Tuple of (march_2025_df, september_2024_df)
    """
    print("Loading FedScope Employment Data...")
    print("=" * 50)
    
    # GitHub URLs for the parquet files
    base_url = "https://github.com/abigailhaddad/fedscope_employment/raw/main/fedscope_data/parquet/"
    
    march_url = f"{base_url}fedscope_employment_March_2025.parquet"
    sept_url = f"{base_url}fedscope_employment_September_2024.parquet"
    
    try:
        print("📥 Loading March 2025 data...")
        df_march = pd.read_parquet(march_url)
        print(f"   ✓ Loaded {len(df_march):,} records")
        
        print("📥 Loading September 2024 data...")
        df_sept = pd.read_parquet(sept_url)
        print(f"   ✓ Loaded {len(df_sept):,} records")
        
        print(f"\n📊 Data Summary:")
        print(f"   March 2025: {len(df_march):,} employees")
        print(f"   September 2024: {len(df_sept):,} employees")
        print(f"   Columns: {df_march.shape[1]}")
        
        return df_march, df_sept
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None, None

# =============================================================================
# CELL 2: Great Tables Default Format Configuration
# =============================================================================

def create_default_gt_format() -> dict:
    """
    Create a default configuration for great-tables formatting that can be reused.
    
    Returns:
        Dictionary with default formatting options
    """
    return {
        'table_font_size': '12px',
        'heading_title_font_size': '16px', 
        'heading_subtitle_font_size': '14px',
        'number_decimals': 0,
        'percent_decimals': 1,
        'color_palette': ['#e53e3e', '#ffffff', '#38a169'],  # red, white, green
        'show_thousands_sep': True,
        'force_percent_sign': True
    }

# =============================================================================
# CELL 3: Standalone Data Cleaning Functions
# =============================================================================

def clean_agency_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean agency names by removing prefixes from September 2024 data.
    September 2024 format: 'VATA-VETERANS HEALTH ADMINISTRATION'
    March 2025 format: 'VETERANS HEALTH ADMINISTRATION'
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned agency names
    """
    df_clean = df.copy()
    df_clean['agysubt'] = df_clean['agysubt'].astype(str).str.replace(r'^[A-Z0-9]+-', '', regex=True)
    return df_clean

def clean_education_level_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean education level names by removing numeric prefixes from September 2024 data.
    September 2024 format: '13-BACHELOR\'S DEGREE'
    March 2025 format: 'BACHELOR\'S DEGREE'
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned education level names
    """
    df_clean = df.copy()
    # Only remove numeric prefixes (1-2 digits followed by dash)
    df_clean['edlvlt'] = df_clean['edlvlt'].astype(str).str.replace(r'^\d{1,2}-', '', regex=True)
    # Also remove ** prefix
    df_clean['edlvlt'] = df_clean['edlvlt'].astype(str).str.replace(r'^\*\*-', '', regex=True)
    return df_clean

def clean_appointment_type_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean appointment type names by removing numeric prefixes from September 2024 data.
    September 2024 format: '10-Competitive Service - Career'
    March 2025 format: 'CAREER (COMPETITIVE SERVICE PERMANENT)'
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned appointment type names
    """
    df_clean = df.copy()
    # Only remove numeric prefixes (1-2 digits followed by dash)
    df_clean['toat'] = df_clean['toat'].astype(str).str.replace(r'^\d{1,2}-', '', regex=True)
    # Also remove ** prefix
    df_clean['toat'] = df_clean['toat'].astype(str).str.replace(r'^\*\*-', '', regex=True)
    return df_clean

def clean_occupation_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean occupation names by removing prefixes from September 2024 data.
    September 2024 format: '0610-NURSE'
    March 2025 format: 'NURSE'
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned occupation names
    """
    df_clean = df.copy()
    df_clean['occt'] = df_clean['occt'].astype(str).str.replace(r'^[A-Z0-9]+-', '', regex=True)
    return df_clean

def clean_category_names(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Generic function to clean category names by removing prefixes.
    September 2024 data has prefixes like 'VATA-' or '0610-' that need to be removed.
    
    Args:
        df: DataFrame to clean
        column: Column name to clean
        
    Returns:
        DataFrame with cleaned category names
    """
    df_clean = df.copy()
    
    # For education and appointment types, use more specific cleaning
    if column == 'edlvlt':
        df_clean = clean_education_level_names(df_clean)
    elif column == 'toat':
        df_clean = clean_appointment_type_names(df_clean)
    else:
        # For other fields like agencies, use the original broader cleaning
        df_clean[column] = df_clean[column].astype(str).str.replace(r'^[A-Z0-9]+-', '', regex=True)
    
    return df_clean

# =============================================================================
# CELL 4: Helper Functions for Table Creation
# =============================================================================

def create_comparison_table(df_march: pd.DataFrame, df_sept: pd.DataFrame, 
                          group_column: str, title: str, filter_func=None, 
                          custom_sort_func=None, clean_names=True, text_column=None) -> pd.DataFrame:
    """
    Create a comparison table between March 2025 and September 2024 data.
    
    Args:
        df_march: March 2025 data
        df_sept: September 2024 data
        group_column: Column to group by (code field)
        title: Title for the table
        filter_func: Optional function to filter data before grouping
        text_column: Text description column to add readable names
    
    Returns:
        DataFrame with comparison data
    """
    # Apply filter if provided
    if filter_func:
        df_march_filtered = df_march[filter_func(df_march)].copy()
        df_sept_filtered = df_sept[filter_func(df_sept)].copy()
    else:
        df_march_filtered = df_march.copy()
        df_sept_filtered = df_sept.copy()
    
    # Clean category names only if needed (agencies and occupations have prefixes)
    # September 2024 data has prefixes like 'VATA-' or '0610-' that need to be removed
    if clean_names:
        df_march_filtered = clean_category_names(df_march_filtered, group_column)
        df_sept_filtered = clean_category_names(df_sept_filtered, group_column)
    
    # Convert employment to numeric (handling string values)
    df_march_filtered['employment_num'] = pd.to_numeric(
        df_march_filtered['employment'].replace(['REDACTED', '*****', 'nan', ''], '1'), 
        errors='coerce'
    ).fillna(1)
    
    df_sept_filtered['employment_num'] = pd.to_numeric(
        df_sept_filtered['employment'].replace(['REDACTED', '*****', 'nan', ''], '1'), 
        errors='coerce'
    ).fillna(1)
    
    # Group and sum employment counts
    march_counts = df_march_filtered.groupby(group_column)['employment_num'].sum().round().astype(int)
    sept_counts = df_sept_filtered.groupby(group_column)['employment_num'].sum().round().astype(int)
    
    # Get all unique categories from both periods
    all_categories = sorted(set(march_counts.index) | set(sept_counts.index))
    
    # Create name mapping using March 2025 names first, then September 2024 as fallback
    name_mapping = {}
    if text_column:
        # Get March 2025 code-to-name mapping
        march_mapping = df_march_filtered.groupby(group_column)[text_column].first().to_dict()
        # Get September 2024 code-to-name mapping (cleaned if needed)
        if clean_names:
            df_sept_clean = clean_category_names(df_sept_filtered, text_column)
            sept_mapping = df_sept_clean.groupby(group_column)[text_column].first().to_dict()
        else:
            sept_mapping = df_sept_filtered.groupby(group_column)[text_column].first().to_dict()
        
        # Create combined mapping: use March 2025 name if available, otherwise use September 2024
        for category in all_categories:
            if category in march_mapping and march_mapping[category] not in ['REDACTED', '', 'nan']:
                name_mapping[category] = march_mapping[category]
            elif category in sept_mapping and sept_mapping[category] not in ['REDACTED', '', 'nan']:
                name_mapping[category] = sept_mapping[category]
            else:
                name_mapping[category] = str(category)  # Use code as fallback
    
    # Create comparison dataframe with proper matching
    comparison_data = []
    redacted_data = None
    
    for category in all_categories:
        march_val = march_counts.get(category, 0)
        sept_val = sept_counts.get(category, 0)
        
        # Get readable name
        category_name = name_mapping.get(category, str(category)) if text_column else str(category)
        
        # Handle REDACTED separately to put at bottom
        if category == 'REDACTED' or category_name == 'REDACTED':
            if text_column:
                redacted_data = {
                    'Code': category,
                    'Category': category_name,
                    'Sep_2024': sept_val,
                    'Mar_2025': march_val
                }
            else:
                redacted_data = {
                    'Category': category_name,
                    'Sep_2024': sept_val,
                    'Mar_2025': march_val
                }
        else:
            if text_column:
                comparison_data.append({
                    'Code': category,
                    'Category': category_name,
                    'Sep_2024': sept_val,
                    'Mar_2025': march_val
                })
            else:
                comparison_data.append({
                    'Category': category_name,
                    'Sep_2024': sept_val,
                    'Mar_2025': march_val
                })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Add REDACTED at the bottom if it exists
    if redacted_data:
        comparison_df = pd.concat([comparison_df, pd.DataFrame([redacted_data])], ignore_index=True)
    
    # Calculate changes
    comparison_df['Change'] = comparison_df['Mar_2025'] - comparison_df['Sep_2024']
    comparison_df['Pct_Change'] = np.where(
        comparison_df['Sep_2024'] > 0,
        (comparison_df['Change'] / comparison_df['Sep_2024']) * 100,
        np.where(comparison_df['Mar_2025'] > 0, 100, 0)
    )
    
    # Add totals row at the bottom
    total_sept = comparison_df['Sep_2024'].sum()
    total_march = comparison_df['Mar_2025'].sum()
    total_change = total_march - total_sept
    total_pct_change = (total_change / total_sept) * 100 if total_sept > 0 else 0
    
    # Create totals row
    if text_column:
        totals_row = {
            'Code': 'TOTAL',
            'Category': 'TOTAL',
            'Sep_2024': total_sept,
            'Mar_2025': total_march,
            'Change': total_change,
            'Pct_Change': total_pct_change
        }
    else:
        totals_row = {
            'Category': 'TOTAL',
            'Sep_2024': total_sept,
            'Mar_2025': total_march,
            'Change': total_change,
            'Pct_Change': total_pct_change
        }
    
    # Add totals row to dataframe
    comparison_df = pd.concat([comparison_df, pd.DataFrame([totals_row])], ignore_index=True)
    
    # Apply custom sorting if provided, otherwise sort by March 2025 count
    if custom_sort_func:
        comparison_df = custom_sort_func(comparison_df)
    else:
        comparison_df = comparison_df.sort_values('Mar_2025', ascending=False)
    
    return comparison_df

def format_comparison_table(df: pd.DataFrame, title: str, custom_format: dict = None) -> GT:
    """
    Format a comparison dataframe using great_tables with configurable formatting.
    
    Args:
        df: Comparison dataframe
        title: Table title
        custom_format: Optional custom formatting dict, uses default if None
    
    Returns:
        GT table object
    """
    # Use default format if none provided
    fmt = custom_format if custom_format else create_default_gt_format()
    
    # Reset index to avoid indexing issues with great_tables
    df_clean = df.reset_index(drop=True)
    
    # Calculate color domains excluding TOTAL rows (handle empty dataframes)
    non_total_df = df_clean[df_clean['Category'] != 'TOTAL']
    change_max = non_total_df['Change'].abs().max() if len(non_total_df) > 0 else 1
    pct_max = non_total_df['Pct_Change'].abs().max() if len(non_total_df) > 0 else 1
    
    # Set up column labels based on whether we have a Code column
    if 'Code' in df_clean.columns:
        col_labels = {
            'Code': "Code",
            'Category': "Category",
            'Sep_2024': "Sep 2024",
            'Mar_2025': "Mar 2025", 
            'Change': "Change",
            'Pct_Change': "% Change"
        }
        right_align_cols = ["Code", "Sep_2024", "Mar_2025", "Change", "Pct_Change"]
    else:
        col_labels = {
            'Category': "Category",
            'Sep_2024': "Sep 2024",
            'Mar_2025': "Mar 2025", 
            'Change': "Change",
            'Pct_Change': "% Change"
        }
        right_align_cols = ["Sep_2024", "Mar_2025", "Change", "Pct_Change"]
    
    gt_table = (
        GT(df_clean)
        .tab_header(
            title=html(f"<strong>{title}</strong>"),
            subtitle="September 2024 vs March 2025 Comparison"
        )
        .cols_label(**col_labels)
        .fmt_number(
            columns=["Sep_2024", "Mar_2025", "Change"],
            use_seps=fmt['show_thousands_sep'],
            decimals=fmt['number_decimals']
        )
        .fmt_number(
            columns=["Pct_Change"],
            decimals=fmt['percent_decimals'],
            force_sign=fmt['force_percent_sign']
        )
        .data_color(
            columns=["Change"],
            palette=fmt['color_palette'],
            domain=[-change_max, change_max],
            rows=lambda df: df['Category'] != 'TOTAL'
        )
        .data_color(
            columns=["Pct_Change"],
            palette=fmt['color_palette'],
            domain=[-pct_max, pct_max],
            rows=lambda df: df['Category'] != 'TOTAL'
        )
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.column_labels()
        )
        .tab_style(
            style=style.text(align="right"),
            locations=loc.body(columns=right_align_cols)
        )
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(rows=lambda df: df['Category'] == 'TOTAL')
        )
        .tab_options(
            table_font_size=fmt['table_font_size'],
            heading_title_font_size=fmt['heading_title_font_size'],
            heading_subtitle_font_size=fmt['heading_subtitle_font_size']
        )
    )
    
    return gt_table

# =============================================================================
# CELL 4: Redaction Overview Chart
# =============================================================================

def create_redaction_overview_chart(df_march: pd.DataFrame) -> GT:
    """
    Create a chart showing redaction percentages by variable for March 2025.
    """
    print("\n" + "=" * 60)
    print("CREATING REDACTION OVERVIEW CHART")
    print("=" * 60)
    
    # Variables to check for redaction
    redaction_fields = {
        'agelvlt': 'Age Level',
        'edlvlt': 'Education Level', 
        'toat': 'Type of Appointment',
        'agysubt': 'Agency',
        'loct': 'Location',
        'occt': 'Occupation',
        'salary': 'Salary',
        'supervist': 'Supervisory Status',
        'wrkscht': 'Work Schedule'
    }
    
    # Calculate redaction percentages
    redaction_data = []
    total_records = len(df_march)
    
    for field, display_name in redaction_fields.items():
        if field in df_march.columns:
            redacted_count = (df_march[field] == 'REDACTED').sum()
            redaction_pct = (redacted_count / total_records) * 100
            
            redaction_data.append({
                'Variable': display_name,
                'Redacted_Count': redacted_count,
                'Total_Records': total_records,
                'Redaction_Percentage': redaction_pct
            })
    
    # Create dataframe and sort by redaction percentage
    redaction_df = pd.DataFrame(redaction_data)
    redaction_df = redaction_df.sort_values('Redaction_Percentage', ascending=False)
    
    print(f"✓ Redaction overview created for {len(redaction_df)} variables")
    
    # Format as great_tables
    redaction_table = (
        GT(redaction_df)
        .tab_header(
            title=html("<strong>March 2025 Data Redaction Overview</strong>"),
            subtitle="Percentage of records with REDACTED values by variable"
        )
        .cols_label(
            Variable="Variable",
            Redacted_Count="Redacted Records",
            Total_Records="Total Records",
            Redaction_Percentage="% Redacted"
        )
        .fmt_number(
            columns=["Redacted_Count", "Total_Records"],
            use_seps=True,
            decimals=0
        )
        .fmt_number(
            columns=["Redaction_Percentage"],
            decimals=1
        )
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.column_labels()
        )
        .tab_style(
            style=style.text(align="right"),
            locations=loc.body(columns=["Redacted_Count", "Total_Records", "Redaction_Percentage"])
        )
        .tab_options(
            table_font_size="12px",
            heading_title_font_size="16px",
            heading_subtitle_font_size="14px"
        )
    )
    
    return redaction_table

# =============================================================================
# CELL 5: Age Groups Comparison Table
# =============================================================================

def create_age_groups_table(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> GT:
    """
    Create age groups comparison table sorted by age (20-24 first, REDACTED last).
    """
    print("\n" + "=" * 60)
    print("CREATING AGE GROUPS COMPARISON TABLE")
    print("=" * 60)
    
    def sort_age_groups(df):
        """Custom sort function for age groups"""
        # Define age group order 
        age_order = ['Less than 20', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                    '50-54', '55-59', '60-64', '65 or more', 'Unspecified', 'REDACTED']
        
        # Create a mapping for sorting
        order_map = {age: i for i, age in enumerate(age_order)}
        
        # Add sort key
        df['sort_key'] = df['Category'].map(lambda x: order_map.get(x, 999))
        
        # Sort and remove sort key
        df_sorted = df.sort_values('sort_key').drop('sort_key', axis=1)
        return df_sorted
    
    # Create comparison data with custom sorting (don't clean age group names, no text column needed)
    age_comparison = create_comparison_table(
        df_march, df_sept, 
        group_column='agelvlt',
        title="Age Groups Comparison",
        custom_sort_func=sort_age_groups,
        clean_names=False,
        text_column=None
    )
    
    print(f"✓ Age groups comparison created with {len(age_comparison)} categories")
    
    # Format as great_tables (show all age groups)
    age_table = format_comparison_table(
        age_comparison,
        "Federal Employment by Age Groups"
    )
    
    return age_table

# =============================================================================
# CELL 6: Education Level Comparison Table
# =============================================================================

def create_education_level_table(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> GT:
    """
    Create education level comparison table sorted from least to most education.
    """
    print("\n" + "=" * 60)
    print("CREATING EDUCATION LEVEL COMPARISON TABLE")
    print("=" * 60)
    
    def sort_education_levels(df):
        """Custom sort function for education levels (least to most education)"""
        # Define education order from least to most education
        education_order = [
            'NO FORMAL EDUCATION OR SOME ELEMENTARY SCHOOL - DID NOT COMPLETE',
            'ELEMENTARY SCHOOL COMPLETED - NO HIGH SCHOOL',
            'SOME HIGH SCHOOL - DID NOT COMPLETE',
            'HIGH SCHOOL GRADUATE OR CERTIFICATE OF EQUIVALENCY',
            'TERMINAL OCCUPATIONAL PROGRAM - DID NOT COMPLETE',
            'TERMINAL OCCUPATIONAL PROGRAM - CERTIFICATE OF COMPLETION, DIPLOMA OR EQUIVALENT',
            'SOME COLLEGE - LESS THAN ONE YEAR',
            'ONE YEAR COLLEGE',
            'TWO YEARS COLLEGE',
            'ASSOCIATE DEGREE',
            'THREE YEARS COLLEGE',
            'FOUR YEARS COLLEGE',
            'BACHELOR\'S DEGREE',
            'POST-BACHELOR\'S',
            'MASTER\'S DEGREE',
            'POST-MASTER\'S',
            'SIXTH-YEAR DEGREE',
            'POST-SIXTH YEAR',
            'FIRST PROFESSIONAL',
            'POST-FIRST PROFESSIONAL',
            'DOCTORATE DEGREE',
            'POST-DOCTORATE',
            'Invalid',
            'No Data Reported',
            'REDACTED'
        ]
        
        # Create a mapping for sorting
        order_map = {edu: i for i, edu in enumerate(education_order)}
        
        # Separate TOTAL row
        total_mask = df['Category'] == 'TOTAL'
        total_row = df[total_mask]
        non_total_df = df[~total_mask]
        
        # Add sort key to non-total rows
        non_total_df['sort_key'] = non_total_df['Category'].map(lambda x: order_map.get(x, 999))
        
        # Sort and remove sort key
        non_total_sorted = non_total_df.sort_values('sort_key').drop('sort_key', axis=1)
        
        # Concatenate with TOTAL at bottom
        if len(total_row) > 0:
            return pd.concat([non_total_sorted, total_row], ignore_index=True)
        else:
            return non_total_sorted
    
    # Create comparison data using education level codes (no cleaning needed)
    education_comparison = create_comparison_table(
        df_march, df_sept,
        group_column='edlvl',
        title="Education Level Comparison",
        custom_sort_func=sort_education_levels,
        clean_names=True,
        text_column='edlvlt'
    )
    
    print(f"✓ Education level comparison created with {len(education_comparison)} categories")
    
    # Format as great_tables (show all education levels)
    education_table = format_comparison_table(
        education_comparison,
        "Federal Employment by Education Level"
    )
    
    return education_table

# =============================================================================
# CELL 7: Type of Appointment Comparison Table
# =============================================================================

def create_appointment_type_table(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> GT:
    """
    Create type of appointment comparison table with REDACTED at bottom.
    """
    print("\n" + "=" * 60)
    print("CREATING TYPE OF APPOINTMENT COMPARISON TABLE")
    print("=" * 60)
    
    def sort_appointments(df):
        """Custom sort function to put REDACTED and TOTAL at bottom"""
        # Separate TOTAL and REDACTED from other appointment types
        total_mask = df['Category'] == 'TOTAL'
        redacted_mask = df['Category'] == 'REDACTED'
        special_mask = total_mask | redacted_mask
        
        total_rows = df[total_mask]
        redacted_rows = df[redacted_mask]
        regular_rows = df[~special_mask]
        
        # Sort regular rows by March 2025 count (descending)
        regular_sorted = regular_rows.sort_values('Mar_2025', ascending=False)
        
        # Concatenate: regular rows, then REDACTED, then TOTAL at bottom
        result_parts = [regular_sorted]
        if len(redacted_rows) > 0:
            result_parts.append(redacted_rows)
        if len(total_rows) > 0:
            result_parts.append(total_rows)
            
        return pd.concat(result_parts, ignore_index=True)
    
    # Create comparison data using appointment type codes (no cleaning needed)
    appointment_comparison = create_comparison_table(
        df_march, df_sept,
        group_column='toa',
        title="Type of Appointment Comparison",
        custom_sort_func=sort_appointments,
        clean_names=True,
        text_column='toat'
    )
    
    print(f"✓ Type of appointment comparison created with {len(appointment_comparison)} categories")
    
    # Format as great_tables (show all appointment types)
    appointment_table = format_comparison_table(
        appointment_comparison,
        "Federal Employment by Type of Appointment"
    )
    
    return appointment_table

# =============================================================================
# CELL 8: Working Around Redaction - Modular Functions
# =============================================================================

def get_agencies_with_redacted_data(df: pd.DataFrame, field: str) -> list:
    """
    Identify agencies that have redacted data for a specific field.
    
    Args:
        df: DataFrame to analyze (typically March 2025 data)
        field: Field name to check for redaction (e.g., 'agelvlt', 'edlvlt', 'loct')
    
    Returns:
        List of agency codes that have redacted data in the specified field
    """
    redacted_agencies = df[df[field] == 'REDACTED']['agy'].unique()
    return sorted(redacted_agencies.tolist())

def filter_out_redacted_agencies(df_march: pd.DataFrame, df_sept: pd.DataFrame, 
                                redacted_agencies: list) -> tuple:
    """
    Filter out agencies with redacted data from both datasets.
    
    Args:
        df_march: March 2025 data
        df_sept: September 2024 data  
        redacted_agencies: List of agency codes to exclude
    
    Returns:
        Tuple of (filtered_march_df, filtered_sept_df)
    """
    df_march_clean = df_march[~df_march['agy'].isin(redacted_agencies)].copy()
    df_sept_clean = df_sept[~df_sept['agy'].isin(redacted_agencies)].copy()
    
    return df_march_clean, df_sept_clean

def create_age_groups_table_excluding_redacted_agencies(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> tuple:
    """
    Example: Create age groups comparison table excluding agencies that have redacted age data.
    This demonstrates how to work around redaction issues.
    """
    print("\\n" + "=" * 60)
    print("CREATING AGE GROUPS TABLE - EXCLUDING REDACTED AGENCIES")
    print("=" * 60)
    
    # Step 1: Identify agencies with redacted age data in March 2025
    print("Step 1: Identifying agencies with redacted age data in March 2025...")
    redacted_agencies = get_agencies_with_redacted_data(df_march, 'agelvlt')
    print(f"   Found {len(redacted_agencies)} agencies with redacted age data")
    print(f"   Agency codes: {redacted_agencies[:10]}{'...' if len(redacted_agencies) > 10 else ''}")
    
    # Step 2: Filter out these agencies from both datasets
    print("\\nStep 2: Filtering out redacted agencies from both datasets...")
    df_march_clean, df_sept_clean = filter_out_redacted_agencies(df_march, df_sept, redacted_agencies)
    
    print(f"   March 2025: {len(df_march):,} → {len(df_march_clean):,} records ({len(df_march_clean)/len(df_march)*100:.1f}% retained)")
    print(f"   September 2024: {len(df_sept):,} → {len(df_sept_clean):,} records ({len(df_sept_clean)/len(df_sept)*100:.1f}% retained)")
    
    # Step 3: Create comparison table with cleaned data
    print("\\nStep 3: Creating age groups comparison with clean data...")
    
    def sort_age_groups(df):
        """Custom sort function for age groups"""
        # Separate TOTAL row
        total_mask = df['Category'] == 'TOTAL'
        total_row = df[total_mask]
        non_total_df = df[~total_mask]
        
        age_order = ['Less than 20', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                    '50-54', '55-59', '60-64', '65 or more', 'Unspecified', 'REDACTED']
        order_map = {age: i for i, age in enumerate(age_order)}
        non_total_df['sort_key'] = non_total_df['Category'].map(lambda x: order_map.get(x, 999))
        non_total_sorted = non_total_df.sort_values('sort_key').drop('sort_key', axis=1)
        
        # Concatenate with TOTAL at bottom
        if len(total_row) > 0:
            return pd.concat([non_total_sorted, total_row], ignore_index=True)
        else:
            return non_total_sorted
    
    # Create comparison data using cleaned datasets
    age_comparison_clean = create_comparison_table(
        df_march_clean, df_sept_clean, 
        group_column='agelvlt',
        title="Age Groups Comparison (Excluding Redacted Agencies)",
        custom_sort_func=sort_age_groups,
        clean_names=False,
        text_column=None
    )
    
    print(f"✓ Clean age groups comparison created with {len(age_comparison_clean)} categories")
    
    # Format as great_tables with footnote about excluded agencies
    age_table_clean = format_comparison_table(
        age_comparison_clean,
        "Federal Employment by Age Groups (Excluding Redacted Agencies)"
    )
    
    # Add footnote about excluded agencies with full names
    # Get agency code to name mapping from September 2024 data (less redacted)
    agency_mapping = df_sept.groupby('agy')['agysubt'].first().to_dict()
    excluded_agency_names = []
    for code in redacted_agencies:
        sub_agency_name = agency_mapping.get(code, code)
        
        # Extract main agency name from sub-agency name
        if isinstance(sub_agency_name, str):
            # Common patterns: "AIR FORCE ...", "U.S. ARMY ...", "NAVAL ...", etc.
            if sub_agency_name.startswith('AF') and '-' in sub_agency_name:
                # Handle format like "AF02-AIR FORCE INSPECTION AGENCY"
                main_name = sub_agency_name.split('-', 1)[1].split(' ')[0:2]  # Get "AIR FORCE"
                main_name = ' '.join(main_name)
            elif 'AIR FORCE' in sub_agency_name:
                main_name = 'AIR FORCE'
            elif 'ARMY' in sub_agency_name:
                main_name = 'ARMY'
            elif 'NAVAL' in sub_agency_name or 'NAVY' in sub_agency_name:
                main_name = 'NAVY'
            elif code == 'DD':
                main_name = 'DEPARTMENT OF DEFENSE'
            else:
                main_name = sub_agency_name
        else:
            main_name = code
            
        excluded_agency_names.append(f"{code} ({main_name})")
    
    excluded_agencies_text = f"Excluded agencies: {', '.join(excluded_agency_names)}"
    age_table_clean = age_table_clean.tab_source_note(
        source_note=excluded_agencies_text
    )
    
    return age_table_clean, redacted_agencies

# =============================================================================
# CELL 9: Display Tables and Summary
# =============================================================================

def display_tables_and_summary(redaction_chart: GT, age_table: GT, education_table: GT, appointment_table: GT, age_table_clean: GT = None, excluded_agencies: list = None):
    """
    Display all tables and provide a summary.
    """
    print("\n" + "=" * 60)
    print("DISPLAYING ANALYSIS RESULTS")
    print("=" * 60)
    
    print("\n🔒 REDACTION OVERVIEW CHART")
    print("-" * 40)
    redaction_chart.show()
    
    print("\n📊 TABLE 1: AGE GROUPS COMPARISON")
    print("-" * 40)
    age_table.show()
    
    print("\n🎓 TABLE 2: EDUCATION LEVEL COMPARISON")
    print("-" * 40)
    education_table.show()
    
    print("\n📋 TABLE 3: TYPE OF APPOINTMENT COMPARISON")
    print("-" * 40)
    appointment_table.show()
    
    if age_table_clean:
        print("\\n🔧 TABLE 4: AGE GROUPS EXCLUDING REDACTED AGENCIES")
        print("-" * 40)
        age_table_clean.show()
        if excluded_agencies:
            print(f"\\n   Excluded agencies with redacted age data: {', '.join(excluded_agencies)}")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    table_count = "Four" if not age_table_clean else "Five"
    print(f"✅ {table_count} analysis components have been generated using great-tables:")
    print("   0. Redaction Overview - Shows percentage of REDACTED values by variable")
    print("   1. Age Groups - Sorted by age (Less than 20 first), REDACTED at bottom")
    print("   2. Education Level - Sorted from least to most education, REDACTED at bottom")
    print("   3. Type of Appointment - Sorted by employee count, REDACTED at bottom")
    if age_table_clean:
        print("   4. Age Groups (Clean) - Excluding agencies with redacted age data")
    print("\n💡 Each table shows:")
    print("   • September 2024 counts")
    print("   • March 2025 counts") 
    print("   • Absolute change")
    print("   • Percentage change")
    print("   • Color coding for increases (green) and decreases (red)")
    print("\n📝 To convert to Jupyter notebook:")
    print("   Copy each cell section into separate notebook cells")

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """
    Execute the complete FedScope comparison workflow.
    """
    print("🚀 FedScope Employment Comparison: September 2024 vs March 2025")
    print("=" * 70)
    print("Creating three comparison tables using great-tables:")
    print("1. Age Groups (sorted by age, REDACTED at bottom)")
    print("2. Education Level (sorted least to most, REDACTED at bottom)")
    print("3. Type of Appointment (sorted by count, REDACTED at bottom)")
    print("=" * 70)
    
    # Load data
    df_march, df_sept = load_fedscope_data()
    
    if df_march is None or df_sept is None:
        print("❌ Failed to load data. Exiting.")
        return
    
    try:
        # Create the redaction overview chart
        redaction_chart = create_redaction_overview_chart(df_march)
        
        # Create the three comparison tables
        age_table = create_age_groups_table(df_march, df_sept)
        education_table = create_education_level_table(df_march, df_sept)
        appointment_table = create_appointment_type_table(df_march, df_sept)
        
        # Create example of working around redaction
        age_table_clean, excluded_agencies = create_age_groups_table_excluding_redacted_agencies(df_march, df_sept)
        
        # Display tables and summary
        display_tables_and_summary(redaction_chart, age_table, education_table, appointment_table, age_table_clean, excluded_agencies)
        
        print(f"\n✅ Analysis complete!")
        print(f"💾 Tables are ready for display in Jupyter notebooks or web apps")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()