"""
FedScope Employment Data Analysis: March 2025 vs September 2024
==============================================================

This script demonstrates how to work with FedScope Employment data, focusing on:
1. Loading March 2025 and September 2024 data from GitHub
2. Analyzing redaction patterns between the two periods
3. Deep dive into medical professions (0600 occupation codes)
4. Comparative analysis between the two time periods

Each section is structured to mimic Jupyter notebook cells for easy conversion.

Data Sources:
- March 2025: fedscope_employment_March_2025.parquet 
- September 2024: fedscope_employment_September_2024.parquet

Note: March 2025 data includes employees on leave and has increased redaction
due to OPM's Data Release Policy.
"""

import pandas as pd
import numpy as np
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
# CELL 2: Data Overview and Structure
# =============================================================================

def explore_data_structure(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> None:
    """
    Explore the basic structure and content of both datasets.
    """
    print("\n" + "=" * 60)
    print("DATA STRUCTURE EXPLORATION")
    print("=" * 60)
    
    print("\n📋 Column Overview:")
    print(f"   Total columns: {len(df_march.columns)}")
    print(f"   Sample columns: {list(df_march.columns[:10])}")
    
    print("\n📅 Time Period Coverage:")
    march_periods = df_march[['year', 'quarter']].drop_duplicates().sort_values(['year', 'quarter'])
    sept_periods = df_sept[['year', 'quarter']].drop_duplicates().sort_values(['year', 'quarter'])
    
    print(f"   March data covers: {march_periods.values.tolist()}")
    print(f"   September data covers: {sept_periods.values.tolist()}")
    
    print("\n📊 Data Types Sample:")
    key_columns = ['agysubt', 'loct', 'occt', 'salary', 'employment']
    for col in key_columns:
        if col in df_march.columns:
            sample_values = df_march[col].dropna().unique()[:3]
            print(f"   {col}: {sample_values} (type: {df_march[col].dtype})")

# =============================================================================
# CELL 3: Redaction Analysis
# =============================================================================

def analyze_redaction_patterns(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> pd.DataFrame:
    """
    Compare redaction patterns between March 2025 and September 2024.
    
    Returns:
        DataFrame with redaction statistics by column
    """
    print("\n" + "=" * 60)
    print("REDACTION PATTERN ANALYSIS")
    print("=" * 60)
    
    print("\n🔍 Analyzing REDACTED values across all columns...")
    print("   (REDACTED indicates data suppression per OPM's Data Release Policy)")
    
    # Calculate redaction counts for both datasets
    march_redaction = (df_march == 'REDACTED').sum()
    sept_redaction = (df_sept == 'REDACTED').sum()
    
    # Create summary dataframe
    redaction_summary = pd.DataFrame({
        'Column': march_redaction.index,
        'March_2025_Count': march_redaction.values,
        'March_2025_Percent': (march_redaction.values / len(df_march)) * 100,
        'Sept_2024_Count': [sept_redaction.get(col, 0) for col in march_redaction.index],
        'Sept_2024_Percent': [(sept_redaction.get(col, 0) / len(df_sept)) * 100 for col in march_redaction.index]
    })
    
    # Calculate change in redaction
    redaction_summary['Change_in_Count'] = redaction_summary['March_2025_Count'] - redaction_summary['Sept_2024_Count']
    redaction_summary['Change_in_Percent'] = redaction_summary['March_2025_Percent'] - redaction_summary['Sept_2024_Percent']
    
    # Filter to columns with any redaction
    redacted_cols = redaction_summary[
        (redaction_summary['March_2025_Count'] > 0) | 
        (redaction_summary['Sept_2024_Count'] > 0)
    ].sort_values('March_2025_Count', ascending=False)
    
    print(f"\n📈 Redaction Summary:")
    print(f"   Columns with redaction: {len(redacted_cols)}")
    print(f"   Total March 2025 redacted values: {redacted_cols['March_2025_Count'].sum():,}")
    print(f"   Total September 2024 redacted values: {redacted_cols['Sept_2024_Count'].sum():,}")
    
    if len(redacted_cols) > 0:
        print(f"\n📊 Top 10 Most Redacted Columns (March 2025):")
        print("-" * 80)
        print(f"{'Column':<20} {'Mar 2025':<12} {'Mar %':<8} {'Sep 2024':<12} {'Sep %':<8} {'Change':<10}")
        print("-" * 80)
        
        for _, row in redacted_cols.head(10).iterrows():
            print(f"{row['Column']:<20} {row['March_2025_Count']:<12,} "
                  f"{row['March_2025_Percent']:<7.1f}% {row['Sept_2024_Count']:<12,} "
                  f"{row['Sept_2024_Percent']:<7.1f}% {row['Change_in_Count']:<+10,}")
    
    return redacted_cols

# =============================================================================
# CELL 4: Medical Professions Analysis (0600 Occupation Codes)
# =============================================================================

def analyze_medical_professions(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> Dict:
    """
    Deep dive into medical professions (0600 occupation codes).
    
    Returns:
        Dictionary with medical profession analysis results
    """
    print("\n" + "=" * 60)
    print("MEDICAL PROFESSIONS ANALYSIS (0600 OCCUPATION CODES)")
    print("=" * 60)
    
    results = {}
    
    # Filter for medical professions (occupation codes starting with 06)
    # Check both the code field (occ) and description field (occt)
    def is_medical_profession(df):
        """Identify medical profession records"""
        # Look for 06xx codes in occupation field or medical keywords in description
        medical_mask = False
        
        if 'occ' in df.columns:
            medical_mask |= df['occ'].astype(str).str.startswith('06', na=False)
        
        if 'occt' in df.columns:
            medical_keywords = ['MEDICAL', 'PHYSICIAN', 'NURSE', 'DENTIST', 'VETERINAR', 
                              'PHARMACIST', 'THERAPIST', 'RADIOLOGIC', 'CLINICAL']
            for keyword in medical_keywords:
                medical_mask |= df['occt'].astype(str).str.contains(keyword, na=False, case=False)
        
        return medical_mask
    
    # Extract medical profession data
    march_medical = df_march[is_medical_profession(df_march)].copy()
    sept_medical = df_sept[is_medical_profession(df_sept)].copy()
    
    print(f"\n🏥 Medical Profession Overview:")
    print(f"   March 2025: {len(march_medical):,} medical professionals")
    print(f"   September 2024: {len(sept_medical):,} medical professionals")
    print(f"   Change: {len(march_medical) - len(sept_medical):+,} professionals")
    
    results['counts'] = {
        'march_total': len(march_medical),
        'sept_total': len(sept_medical),
        'change': len(march_medical) - len(sept_medical)
    }
    
    # Analyze medical occupations by specific type
    if len(march_medical) > 0 and 'occt' in march_medical.columns:
        print(f"\n📋 Medical Occupation Types (March 2025):")
        march_med_types = march_medical['occt'].value_counts().head(10)
        for occ, count in march_med_types.items():
            print(f"   {occ}: {count:,}")
        
        results['march_occupation_types'] = march_med_types.to_dict()
    
    # Analyze redaction in medical professions
    if len(march_medical) > 0:
        march_med_redaction = (march_medical == 'REDACTED').sum()
        sept_med_redaction = (sept_medical == 'REDACTED').sum() if len(sept_medical) > 0 else pd.Series()
        
        print(f"\n🔒 Redaction in Medical Professions:")
        med_redacted_cols = march_med_redaction[march_med_redaction > 0].sort_values(ascending=False)
        
        if len(med_redacted_cols) > 0:
            print(f"   Columns with redaction: {len(med_redacted_cols)}")
            for col, count in med_redacted_cols.head(5).items():
                sept_count = sept_med_redaction.get(col, 0)
                print(f"   {col}: {count:,} (Sep: {sept_count:,})")
        
        results['medical_redaction'] = med_redacted_cols.to_dict()
    
    # Analyze medical profession salaries (where not redacted)
    for period, df_med, period_name in [
        ('march', march_medical, 'March 2025'), 
        ('sept', sept_medical, 'September 2024')
    ]:
        if len(df_med) > 0 and 'salary' in df_med.columns:
            # Convert salary to numeric, handling redacted/missing values
            df_med['salary_numeric'] = pd.to_numeric(
                df_med['salary'].replace(['REDACTED', '*****', 'nan', ''], np.nan), 
                errors='coerce'
            )
            
            valid_salaries = df_med['salary_numeric'].dropna()
            if len(valid_salaries) > 0:
                print(f"\n💰 Medical Profession Salaries ({period_name}):")
                print(f"   Records with salary data: {len(valid_salaries):,}")
                print(f"   Mean salary: ${valid_salaries.mean():,.2f}")
                print(f"   Median salary: ${valid_salaries.median():,.2f}")
                print(f"   Salary range: ${valid_salaries.min():,.2f} - ${valid_salaries.max():,.2f}")
                
                results[f'{period}_salary_stats'] = {
                    'count': len(valid_salaries),
                    'mean': valid_salaries.mean(),
                    'median': valid_salaries.median(),
                    'min': valid_salaries.min(),
                    'max': valid_salaries.max()
                }
    
    return results

# =============================================================================
# CELL 5: Agency and Location Analysis for Medical Professions
# =============================================================================

def analyze_medical_by_agency_location(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> Dict:
    """
    Analyze medical professions by agency and location.
    """
    print("\n" + "=" * 60)
    print("MEDICAL PROFESSIONS BY AGENCY AND LOCATION")
    print("=" * 60)
    
    results = {}
    
    # Filter for medical professions
    def is_medical_profession(df):
        medical_mask = False
        if 'occ' in df.columns:
            medical_mask |= df['occ'].astype(str).str.startswith('06', na=False)
        if 'occt' in df.columns:
            medical_keywords = ['MEDICAL', 'PHYSICIAN', 'NURSE', 'DENTIST', 'VETERINAR', 
                              'PHARMACIST', 'THERAPIST', 'RADIOLOGIC', 'CLINICAL']
            for keyword in medical_keywords:
                medical_mask |= df['occt'].astype(str).str.contains(keyword, na=False, case=False)
        return medical_mask
    
    march_medical = df_march[is_medical_profession(df_march)].copy()
    sept_medical = df_sept[is_medical_profession(df_sept)].copy()
    
    # Medical professionals by agency
    if len(march_medical) > 0 and 'agysubt' in march_medical.columns:
        print(f"\n🏛️ Top Agencies Employing Medical Professionals (March 2025):")
        
        # Convert employment to numeric and sum by agency
        march_medical['employment_num'] = pd.to_numeric(
            march_medical['employment'].replace(['REDACTED', '*****', 'nan', ''], '1'), 
            errors='coerce'
        ).fillna(1)
        
        agency_med_counts = march_medical.groupby('agysubt')['employment_num'].sum().sort_values(ascending=False)
        
        for agency, count in agency_med_counts.head(10).items():
            print(f"   {agency}: {count:,.0f}")
        
        results['top_agencies'] = agency_med_counts.head(10).to_dict()
    
    # Medical professionals by location
    if len(march_medical) > 0 and 'loct' in march_medical.columns:
        print(f"\n📍 Top Locations for Medical Professionals (March 2025):")
        
        location_med_counts = march_medical.groupby('loct')['employment_num'].sum().sort_values(ascending=False)
        
        for location, count in location_med_counts.head(10).items():
            if location != 'REDACTED':  # Skip redacted locations for display
                print(f"   {location}: {count:,.0f}")
        
        results['top_locations'] = location_med_counts.head(10).to_dict()
    
    # Compare redaction rates between general population and medical professionals
    print(f"\n🔒 Redaction Comparison: General vs Medical Professionals")
    
    # General population redaction rates
    march_general_redaction = (df_march == 'REDACTED').mean() * 100
    march_medical_redaction = (march_medical == 'REDACTED').mean() * 100 if len(march_medical) > 0 else pd.Series()
    
    key_fields = ['loct', 'agysubt', 'salary', 'occt']
    print(f"\n{'Field':<15} {'General Pop %':<15} {'Medical Prof %':<15} {'Difference':<15}")
    print("-" * 60)
    
    for field in key_fields:
        if field in march_general_redaction.index:
            general_rate = march_general_redaction[field]
            medical_rate = march_medical_redaction.get(field, 0)
            difference = medical_rate - general_rate
            print(f"{field:<15} {general_rate:<14.1f}% {medical_rate:<14.1f}% {difference:<+14.1f}%")
    
    return results

# =============================================================================
# CELL 6: Temporal Comparison and Trends
# =============================================================================

def compare_temporal_trends(df_march: pd.DataFrame, df_sept: pd.DataFrame) -> Dict:
    """
    Compare workforce trends between September 2024 and March 2025.
    """
    print("\n" + "=" * 60)
    print("TEMPORAL COMPARISON: SEPTEMBER 2024 vs MARCH 2025")
    print("=" * 60)
    
    results = {}
    
    # Overall workforce change
    total_march = len(df_march)
    total_sept = len(df_sept)
    change = total_march - total_sept
    pct_change = (change / total_sept) * 100
    
    print(f"\n📊 Overall Workforce Change:")
    print(f"   September 2024: {total_sept:,} employees")
    print(f"   March 2025: {total_march:,} employees")
    print(f"   Change: {change:+,} ({pct_change:+.1f}%)")
    
    results['overall_change'] = {
        'sept_total': total_sept,
        'march_total': total_march,
        'change': change,
        'pct_change': pct_change
    }
    
    # Agency-level changes
    if 'agysubt' in df_march.columns and 'agysubt' in df_sept.columns:
        print(f"\n🏛️ Agency-Level Changes (Top 10 by absolute change):")
        
        # Count employees by agency for both periods
        march_agencies = df_march.groupby('agysubt').size()
        sept_agencies = df_sept.groupby('agysubt').size()
        
        # Calculate changes
        agency_changes = (march_agencies - sept_agencies).fillna(march_agencies).fillna(-sept_agencies)
        agency_changes = agency_changes.sort_values(ascending=False)
        
        print(f"\n{'Agency':<50} {'Sep 2024':<12} {'Mar 2025':<12} {'Change':<12}")
        print("-" * 86)
        
        for agency in agency_changes.head(10).index:
            sept_count = sept_agencies.get(agency, 0)
            march_count = march_agencies.get(agency, 0)
            change = march_count - sept_count
            print(f"{agency:<50} {sept_count:<12,} {march_count:<12,} {change:<+12,}")
        
        results['agency_changes'] = agency_changes.head(20).to_dict()
    
    # Check for new redaction patterns
    print(f"\n🔒 New Redaction Patterns:")
    march_redacted_cols = set((df_march == 'REDACTED').any()[lambda x: x].index)
    sept_redacted_cols = set((df_sept == 'REDACTED').any()[lambda x: x].index)
    
    new_redacted = march_redacted_cols - sept_redacted_cols
    reduced_redacted = sept_redacted_cols - march_redacted_cols
    
    if new_redacted:
        print(f"   New columns with redaction in March 2025: {list(new_redacted)}")
    if reduced_redacted:
        print(f"   Columns with reduced redaction in March 2025: {list(reduced_redacted)}")
    if not new_redacted and not reduced_redacted:
        print(f"   No significant changes in redaction patterns between periods")
    
    results['redaction_changes'] = {
        'new_redacted': list(new_redacted),
        'reduced_redacted': list(reduced_redacted)
    }
    
    return results

# =============================================================================
# CELL 7: Summary and Key Findings
# =============================================================================

def generate_summary_report(redaction_analysis: pd.DataFrame, medical_analysis: Dict, 
                          agency_location_analysis: Dict, temporal_analysis: Dict) -> None:
    """
    Generate a comprehensive summary of all analyses.
    """
    print("\n" + "=" * 60)
    print("SUMMARY REPORT: KEY FINDINGS")
    print("=" * 60)
    
    print(f"\n📋 Executive Summary:")
    print(f"   This analysis compared FedScope employment data between September 2024")
    print(f"   and March 2025, with special focus on redaction patterns and medical")
    print(f"   professions (0600 occupation codes).")
    
    print(f"\n🔍 Key Findings:")
    
    # Workforce changes
    if 'overall_change' in temporal_analysis:
        change_data = temporal_analysis['overall_change']
        print(f"   • Total workforce changed by {change_data['change']:+,} employees "
              f"({change_data['pct_change']:+.1f}%)")
    
    # Redaction findings
    if len(redaction_analysis) > 0:
        total_march_redacted = redaction_analysis['March_2025_Count'].sum()
        total_sept_redacted = redaction_analysis['Sept_2024_Count'].sum()
        redaction_change = total_march_redacted - total_sept_redacted
        
        print(f"   • Redacted values increased by {redaction_change:+,} "
              f"({total_march_redacted:,} vs {total_sept_redacted:,})")
        
        most_redacted = redaction_analysis.iloc[0]
        print(f"   • Most redacted field: {most_redacted['Column']} "
              f"({most_redacted['March_2025_Count']:,} values)")
    
    # Medical profession findings
    if 'counts' in medical_analysis:
        med_counts = medical_analysis['counts']
        print(f"   • Medical professionals: {med_counts['march_total']:,} in March 2025 "
              f"(change of {med_counts['change']:+,})")
    
    if 'top_agencies' in agency_location_analysis:
        top_agency = list(agency_location_analysis['top_agencies'].keys())[0]
        top_count = list(agency_location_analysis['top_agencies'].values())[0]
        print(f"   • Top employer of medical professionals: {top_agency} ({top_count:,.0f})")
    
    print(f"\n⚠️  Important Notes:")
    print(f"   • March 2025 data is preliminary and subject to revision")
    print(f"   • Increased redaction reflects OPM's Data Release Policy")
    print(f"   • March 2025 includes employees on various types of leave")
    print(f"   • Medical professions defined as 0600 occupation codes plus keyword matching")
    
    print(f"\n📊 Recommended Next Steps:")
    print(f"   • Monitor redaction patterns in future releases")
    print(f"   • Analyze specific medical specialties within 0600 codes")
    print(f"   • Track agency-specific trends in medical hiring")
    print(f"   • Compare with historical seasonal patterns")

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """
    Execute the complete FedScope analysis workflow.
    """
    print("🚀 FedScope Employment Analysis: March 2025 vs September 2024")
    print("=" * 70)
    print("This analysis focuses on redaction patterns and medical professions")
    print("Data will be loaded directly from GitHub")
    print("=" * 70)
    
    # Load data
    df_march, df_sept = load_fedscope_data()
    
    if df_march is None or df_sept is None:
        print("❌ Failed to load data. Exiting.")
        return
    
    # Run all analyses
    try:
        # Basic exploration
        explore_data_structure(df_march, df_sept)
        
        # Redaction analysis
        redaction_results = analyze_redaction_patterns(df_march, df_sept)
        
        # Medical professions analysis
        medical_results = analyze_medical_professions(df_march, df_sept)
        
        # Agency and location analysis for medical professions
        agency_location_results = analyze_medical_by_agency_location(df_march, df_sept)
        
        # Temporal comparison
        temporal_results = compare_temporal_trends(df_march, df_sept)
        
        # Generate summary
        generate_summary_report(redaction_results, medical_results, 
                              agency_location_results, temporal_results)
        
        print(f"\n✅ Analysis complete! All findings are displayed above.")
        print(f"💡 This script can be easily converted to a Jupyter notebook")
        print(f"   by copying each cell section into separate notebook cells.")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()