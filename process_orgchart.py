"""
Organizational Chart Processing Script
=====================================

This script processes monthly organizational data from SelfProd_2024.xlsx and generates
a comprehensive processed output with promotion/demotion logic, supervisor relationships,
and hierarchical structure management.

Business Rules Implemented:
1. Rank remapping (职级套转): AD→AD1, SBM→BM3, BM→BM1, AS→AS
2. Layer definition: Layer 1 (AD2,AD1), Layer 2 (AD0,BM3,BM2,BM1), Layer 3 (BM0,AS)
3. Assessment months processing (July 202407 & October 202410)
4. Performance-based promotion/demotion logic
5. Supervisor relationship management (DT/GT relationships)
6. New hire/resignation detection
7. Monthly snapshot generation for 202401-202412

Author: Generated Script
Version: 1.0
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# File paths and sheet names
SRC_FILE = "SelfProd_2024.xlsx"
DST_FILE = "SelfProd_2024_processed.xlsx"
DATA_SHEET = "Data"
PROMOTION_SHEET = "PromotionRules"
DEMOTION_SHEET = "DemotionRules"

# Assessment configuration
PROMOTION_WINDOW = 6  # Number of months for performance assessment
ASSESS_MONTHS = {202407, 202410}  # Assessment months (July & October)

# Rank remapping configuration
RANK_MAP = {
    "AD": "AD1",
    "SBM": "BM3", 
    "BM": "BM1",
    "AS": "AS"
}

# Layer definitions
LAYER_MAP = {
    "AD2": 1, "AD1": 1,                    # Layer 1
    "AD0": 2, "BM3": 2, "BM2": 2, "BM1": 2,  # Layer 2
    "BM0": 3, "AS": 3                      # Layer 3
}

# Rank hierarchy for promotion/demotion (ascending order)
RANK_HIERARCHY = ["AS", "BM0", "BM1", "BM2", "BM3", "AD0", "AD1", "AD2"]

# Default promotion/demotion thresholds (fallback if sheets missing)
DEFAULT_PROMOTION_THRESHOLD = 250000  # 承保FYC累计
DEFAULT_MAINTAIN_THRESHOLD = 120000   # 承保FYC累计

# Performance columns for assessment
PERFORMANCE_COLUMNS = [
    '续保率', '承保FYC', '个险折算后FYC', '新单件数', '是否MDRT'
]

# Output columns in required order
OUTPUT_COLUMNS = [
    '营销员代码', '上级主管代码', '总监代码', '总监职级', '总监关系',
    '业务经理代码', '业务经理职级', '业务经理关系',
    '业务主任代码', '业务主任职级', '业务主任关系',
    '一代育成人', '二代育成人'
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def validate_file_exists(filepath: str) -> None:
    """Validate that the source file exists."""
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Source file not found: {filepath}")

def generate_month_range(start_month: int, end_month: int) -> List[int]:
    """
    Generate a list of YYYYMM integers between start and end (inclusive).
    
    Args:
        start_month: Starting month in YYYYMM format
        end_month: Ending month in YYYYMM format
        
    Returns:
        List of month integers in YYYYMM format
    """
    months = []
    year_s, mon_s = divmod(start_month, 100)
    year_e, mon_e = divmod(end_month, 100)
    
    y, m = year_s, mon_s
    while (y < year_e) or (y == year_e and m <= mon_e):
        months.append(y * 100 + m)
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months

def calculate_assessment_window(current_month: int, window_size: int) -> List[int]:
    """
    Calculate the assessment window months for performance evaluation.
    
    Args:
        current_month: Current assessment month (YYYYMM)
        window_size: Number of months to look back
        
    Returns:
        List of months in the assessment window
    """
    year, month = divmod(current_month, 100)
    start_year = year
    start_month = month - window_size + 1
    
    if start_month <= 0:
        start_month += 12
        start_year -= 1
        
    start_yyyymm = start_year * 100 + start_month
    return generate_month_range(start_yyyymm, current_month)

# =============================================================================
# DATA LOADING AND VALIDATION
# =============================================================================

def load_source_data(filepath: str) -> pd.DataFrame:
    """
    Load and validate source data from Excel file.
    
    Args:
        filepath: Path to source Excel file
        
    Returns:
        DataFrame with validated source data
    """
    validate_file_exists(filepath)
    
    # Define data types for consistent handling
    dtype_map = {
        '营销员代码': str,
        '直属主管代码': str,
        '上级主管代码': str,
        'AD代码': str,
        '育成主管代码': str
    }
    
    try:
        xls = pd.ExcelFile(filepath)
        df = xls.parse(DATA_SHEET, dtype=dtype_map, keep_default_na=False)
        print(f"✓ Loaded {len(df)} records from {DATA_SHEET} sheet")
        return df
    except Exception as e:
        raise ValueError(f"Failed to load data from {filepath}: {str(e)}")

def load_promotion_rules(filepath: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load promotion and demotion rules from Excel sheets.
    
    Args:
        filepath: Path to source Excel file
        
    Returns:
        Tuple of (promotion_rules_df, demotion_rules_df)
    """
    try:
        xls = pd.ExcelFile(filepath)
        
        # Try to load promotion rules
        try:
            promo_df = xls.parse(PROMOTION_SHEET)
            print(f"✓ Loaded promotion rules from {PROMOTION_SHEET} sheet")
        except ValueError:
            promo_df = pd.DataFrame()
            print(f"⚠ {PROMOTION_SHEET} sheet not found, using default rules")
            
        # Try to load demotion rules
        try:
            demo_df = xls.parse(DEMOTION_SHEET)
            print(f"✓ Loaded demotion rules from {DEMOTION_SHEET} sheet")
        except ValueError:
            demo_df = pd.DataFrame()
            print(f"⚠ {DEMOTION_SHEET} sheet not found, using default rules")
            
        return promo_df, demo_df
        
    except Exception as e:
        print(f"⚠ Warning: Could not load promotion rules: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

# =============================================================================
# RANK AND LAYER PROCESSING
# =============================================================================

def apply_rank_remapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply rank remapping according to business rules.
    
    Args:
        df: DataFrame with current rank data
        
    Returns:
        DataFrame with remapped ranks and calculated layers
    """
    df = df.copy()
    
    # Apply rank remapping
    df['当前职级'] = df['当前职级'].map(lambda x: RANK_MAP.get(x, x))
    
    # Calculate layer for each rank
    df['RANK_LAYER'] = df['当前职级'].map(lambda x: LAYER_MAP.get(x, None))
    
    # Also remap supervisor ranks if they exist
    if '直属主管职级' in df.columns:
        df['直属主管职级'] = df['直属主管职级'].map(lambda x: RANK_MAP.get(x, x))
    if '上级主管职级' in df.columns:
        df['上级主管职级'] = df['上级主管职级'].map(lambda x: RANK_MAP.get(x, x))
    
    return df

def determine_promotion_decision(performance_data: Dict, promotion_rules: pd.DataFrame, 
                               demotion_rules: pd.DataFrame) -> str:
    """
    Determine promotion/maintain/demotion decision based on performance and rules.
    
    Args:
        performance_data: Dictionary containing performance metrics
        promotion_rules: DataFrame with promotion rules
        demotion_rules: DataFrame with demotion rules
        
    Returns:
        Decision string: '晋升', '维持', or '降级'
    """
    # Use rule-based logic if rules are available
    if not promotion_rules.empty:
        # TODO: Implement complex rule parsing from promotion_rules DataFrame
        # For now, fall back to simple logic
        pass
    
    # Default logic based on 承保FYC累计
    fyc_total = performance_data.get('承保FYC', 0)
    if pd.isna(fyc_total):
        fyc_total = 0
        
    if fyc_total >= DEFAULT_PROMOTION_THRESHOLD:
        return '晋升'
    elif fyc_total >= DEFAULT_MAINTAIN_THRESHOLD:
        return '维持'
    else:
        return '降级'

def apply_rank_adjustment(current_rank: str, decision: str) -> str:
    """
    Apply rank adjustment based on promotion/demotion decision.
    
    Args:
        current_rank: Current rank of the agent
        decision: Promotion decision ('晋升', '维持', '降级')
        
    Returns:
        New rank after adjustment
    """
    if current_rank not in RANK_HIERARCHY:
        return current_rank
        
    current_idx = RANK_HIERARCHY.index(current_rank)
    
    if decision == '晋升' and current_idx < len(RANK_HIERARCHY) - 1:
        return RANK_HIERARCHY[current_idx + 1]
    elif decision == '降级' and current_idx > 0:
        return RANK_HIERARCHY[current_idx - 1]
    else:
        return current_rank

# =============================================================================
# PERFORMANCE ASSESSMENT
# =============================================================================

def calculate_performance_metrics(df: pd.DataFrame, agent_codes: List[str], 
                                assessment_months: List[int]) -> pd.DataFrame:
    """
    Calculate aggregated performance metrics for assessment period.
    
    Args:
        df: Source data DataFrame
        agent_codes: List of agent codes to assess
        assessment_months: List of months in assessment window
        
    Returns:
        DataFrame with aggregated performance metrics
    """
    # Filter data for assessment window
    window_data = df[df['薪资月份'].isin(assessment_months)].copy()
    
    # Group by agent and calculate aggregates
    performance_agg = {}
    
    # Define aggregation rules for each performance column
    agg_rules = {
        '承保FYC': 'sum',
        '个险折算后FYC': 'sum',
        '续保率': 'mean',
        '新单件数': 'sum'
    }
    
    # Calculate aggregations for available columns
    for col, agg_func in agg_rules.items():
        if col in window_data.columns:
            performance_agg[col] = agg_func
    
    if not performance_agg:
        # Return empty DataFrame if no performance columns found
        return pd.DataFrame(index=agent_codes)
    
    # Perform groupby aggregation
    perf_df = window_data.groupby('营销员代码').agg(performance_agg)
    
    # Ensure all agent codes are included (fill missing with 0/NaN)
    perf_df = perf_df.reindex(agent_codes, fill_value=0)
    
    return perf_df

def process_assessment_month(df: pd.DataFrame, current_month: int, 
                           source_data: pd.DataFrame, promotion_rules: pd.DataFrame,
                           demotion_rules: pd.DataFrame) -> pd.DataFrame:
    """
    Process assessment month with promotion/demotion logic.
    
    Args:
        df: Current month DataFrame
        current_month: Current assessment month
        source_data: Full source data for performance calculation
        promotion_rules: Promotion rules DataFrame
        demotion_rules: Demotion rules DataFrame
        
    Returns:
        DataFrame with updated ranks and promotion decisions
    """
    df = df.copy()
    
    # Calculate assessment window
    assessment_months = calculate_assessment_window(current_month, PROMOTION_WINDOW)
    print(f"  Assessment window: {assessment_months}")
    
    # Get performance metrics
    agent_codes = df['营销员代码'].tolist()
    perf_df = calculate_performance_metrics(source_data, agent_codes, assessment_months)
    
    # Merge performance data
    df = df.merge(perf_df, left_on='营销员代码', right_index=True, 
                  how='left', suffixes=('', '_累计'))
    
    # Make promotion decisions
    promotion_decisions = []
    new_ranks = []
    
    for idx, row in df.iterrows():
        # Prepare performance data dictionary
        perf_data = {col: row.get(f"{col}_累计", row.get(col, 0)) 
                    for col in PERFORMANCE_COLUMNS if f"{col}_累计" in row or col in row}
        
        # Determine decision
        decision = determine_promotion_decision(perf_data, promotion_rules, demotion_rules)
        promotion_decisions.append(decision)
        
        # Apply rank adjustment
        new_rank = apply_rank_adjustment(row['当前职级'], decision)
        new_ranks.append(new_rank)
    
    df['升降级标记'] = promotion_decisions
    df['当前职级'] = new_ranks
    
    # Reapply rank remapping to update layers
    df = apply_rank_remapping(df)
    
    print(f"  Processed {len(df)} agents: {promotion_decisions.count('晋升')} promotions, "
          f"{promotion_decisions.count('降级')} demotions, {promotion_decisions.count('维持')} maintained")
    
    return df

# =============================================================================
# SUPERVISOR RELATIONSHIP MANAGEMENT
# =============================================================================

def update_supervisor_relationships(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update supervisor relationships based on layer rules.
    
    Args:
        df: DataFrame with current organizational data
        
    Returns:
        DataFrame with updated supervisor relationships
    """
    df = df.copy()
    
    # Create supervisor lookup for getting supervisor's supervisor
    supervisor_lookup = df.set_index('营销员代码')['上级主管代码'].to_dict()
    
    # Initialize 育成主管代码 if not exists
    if '育成主管代码' not in df.columns:
        df['育成主管代码'] = ''
    
    # Process each agent
    for idx, row in df.iterrows():
        agent_layer = row.get('RANK_LAYER')
        supervisor_code = row['上级主管代码']
        
        if pd.isna(supervisor_code) or supervisor_code == '':
            continue
            
        # Find supervisor's rank and layer
        supervisor_data = df[df['营销员代码'] == supervisor_code]
        if supervisor_data.empty:
            continue
            
        supervisor_rank = supervisor_data.iloc[0]['当前职级']
        supervisor_layer = LAYER_MAP.get(supervisor_rank)
        
        # Check if agent and supervisor are in same layer
        if agent_layer is not None and supervisor_layer == agent_layer:
            # Move supervisor up one level
            supervisor_supervisor = supervisor_lookup.get(supervisor_code, '')
            if supervisor_supervisor:
                df.at[idx, '上级主管代码'] = supervisor_supervisor
                df.at[idx, '育成主管代码'] = supervisor_code
    
    return df

def determine_relationship_type(agent_rank: str) -> str:
    """
    Determine relationship type (DT/GT) based on agent rank.
    
    Args:
        agent_rank: Current rank of the agent
        
    Returns:
        Relationship type: 'DT' or 'GT'
    """
    agent_layer = LAYER_MAP.get(agent_rank, 4)
    return 'DT' if agent_layer >= 3 else 'GT'

def apply_relationship_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply relationship rules and force DT for business managers.
    
    Args:
        df: DataFrame with organizational data
        
    Returns:
        DataFrame with applied relationship rules
    """
    df = df.copy()
    
    # Determine basic relationship types
    df['主管关系'] = df['当前职级'].map(determine_relationship_type)
    
    # Force DT for business manager ranks
    business_manager_ranks = ['AD0', 'BM3', 'BM2', 'BM1']
    manager_mask = df['当前职级'].isin(business_manager_ranks)
    df.loc[manager_mask, '主管关系'] = 'DT'
    
    return df

# =============================================================================
# EMPLOYEE STATUS TRACKING
# =============================================================================

def detect_employee_status_changes(current_agents: Set[str], 
                                 previous_agents: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Detect new hires and resignations by comparing agent sets.
    
    Args:
        current_agents: Set of current month agent codes
        previous_agents: Set of previous month agent codes
        
    Returns:
        Tuple of (new_hires, resignations)
    """
    new_hires = current_agents - previous_agents
    resignations = previous_agents - current_agents
    
    return new_hires, resignations

def apply_employee_status(df: pd.DataFrame, new_hires: Set[str], 
                         resignations: Set[str]) -> pd.DataFrame:
    """
    Apply employee status labels to DataFrame.
    
    Args:
        df: DataFrame with employee data
        new_hires: Set of new hire agent codes
        resignations: Set of resignation agent codes
        
    Returns:
        DataFrame with employee status column
    """
    df = df.copy()
    
    # Default status
    df['员工状态'] = '在职'
    
    # Mark new hires
    df.loc[df['营销员代码'].isin(new_hires), '员工状态'] = '新增'
    
    # Mark resignations (though they typically won't appear in current month data)
    df.loc[df['营销员代码'].isin(resignations), '员工状态'] = '离职'
    
    return df

# =============================================================================
# OUTPUT FORMATTING
# =============================================================================

def create_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create and populate required output columns.
    
    Args:
        df: DataFrame with processed organizational data
        
    Returns:
        DataFrame with all required output columns
    """
    df = df.copy()
    
    # Map basic hierarchy columns
    df['总监代码'] = df.get('AD代码', '')
    df['总监职级'] = 'AD1'  # Default assumption
    df['总监关系'] = 'GT'
    
    df['业务经理代码'] = df.get('直属主管代码', '')
    df['业务经理职级'] = df.get('直属主管职级', '')
    df['业务经理关系'] = df.get('主管关系', 'DT')
    
    df['业务主任代码'] = df.get('上级主管代码', '')
    df['业务主任职级'] = df.get('上级主管职级', '')
    df['业务主任关系'] = df.get('主管关系', 'DT')
    
    # Mentor relationships
    df['一代育成人'] = df.get('育成主管代码', '')
    df['二代育成人'] = ''  # Placeholder for future implementation
    
    return df

def format_final_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format the final output DataFrame with proper column order and data types.
    
    Args:
        df: Processed DataFrame
        
    Returns:
        Formatted DataFrame ready for output
    """
    df = df.copy()
    
    # Ensure all required columns exist
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    
    # Get all original columns plus new output columns
    original_cols = [col for col in df.columns if col not in OUTPUT_COLUMNS]
    final_columns = OUTPUT_COLUMNS + original_cols
    
    # Reorder columns
    df = df.reindex(columns=final_columns)
    
    # Clean up data types
    string_columns = ['营销员代码', '上级主管代码', '总监代码', '业务经理代码', '业务主任代码']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')
    
    return df

# =============================================================================
# MAIN PROCESSING FUNCTION
# =============================================================================

def process_monthly_data(source_data: pd.DataFrame, promotion_rules: pd.DataFrame,
                        demotion_rules: pd.DataFrame) -> pd.DataFrame:
    """
    Process all monthly data according to business rules.
    
    Args:
        source_data: Raw source data from Excel
        promotion_rules: Promotion rules DataFrame
        demotion_rules: Demotion rules DataFrame
        
    Returns:
        Processed DataFrame with all months
    """
    print("Processing monthly organizational data...")
    
    # Get sorted list of all months
    month_list = sorted(source_data['薪资月份'].unique())
    print(f"Processing months: {month_list}")
    
    monthly_snapshots = []
    previous_agent_codes = set()
    
    for month in month_list:
        print(f"\nProcessing month {month}...")
        
        # Extract current month data
        current_df = source_data[source_data['薪资月份'] == month].copy()
        print(f"  {len(current_df)} agents in month {month}")
        
        # Step 1: Apply rank remapping
        current_df = apply_rank_remapping(current_df)
        
        # Step 2: Process assessment if this is an assessment month
        if month in ASSESS_MONTHS:
            print(f"  Processing assessment month {month}")
            current_df = process_assessment_month(
                current_df, month, source_data, promotion_rules, demotion_rules
            )
        
        # Step 3: Detect employee status changes
        current_agent_codes = set(current_df['营销员代码'])
        new_hires, resignations = detect_employee_status_changes(
            current_agent_codes, previous_agent_codes
        )
        
        if new_hires:
            print(f"  {len(new_hires)} new hires detected")
        if resignations:
            print(f"  {len(resignations)} resignations detected")
            
        current_df = apply_employee_status(current_df, new_hires, resignations)
        
        # Step 4: Update supervisor relationships
        current_df = update_supervisor_relationships(current_df)
        current_df = apply_relationship_rules(current_df)
        
        # Step 5: Create output columns
        current_df = create_output_columns(current_df)
        
        # Store snapshot and update previous month tracking
        monthly_snapshots.append(current_df)
        previous_agent_codes = current_agent_codes
    
    # Combine all monthly snapshots
    print(f"\nCombining {len(monthly_snapshots)} monthly snapshots...")
    final_df = pd.concat(monthly_snapshots, ignore_index=True)
    
    # Format final output
    final_df = format_final_output(final_df)
    
    print(f"Final dataset contains {len(final_df)} records")
    return final_df

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """
    Main execution function that orchestrates the entire processing pipeline.
    """
    print("=" * 70)
    print("ORGANIZATIONAL CHART PROCESSING")
    print("=" * 70)
    
    try:
        # Step 1: Load source data
        print("\n1. Loading source data...")
        source_data = load_source_data(SRC_FILE)
        
        # Step 2: Load promotion rules
        print("\n2. Loading promotion/demotion rules...")
        promotion_rules, demotion_rules = load_promotion_rules(SRC_FILE)
        
        # Step 3: Process all monthly data
        print("\n3. Processing monthly data...")
        processed_df = process_monthly_data(source_data, promotion_rules, demotion_rules)
        
        # Step 4: Save output
        print(f"\n4. Saving output to {DST_FILE}...")
        processed_df.to_excel(DST_FILE, index=False, encoding='utf-8')
        
        # Success message
        print("\n" + "=" * 70)
        print(f"✅ SUCCESS! Processing completed successfully.")
        print(f"✅ Output saved to: {DST_FILE}")
        print(f"✅ Total records processed: {len(processed_df):,}")
        print(f"✅ Months covered: {sorted(processed_df['薪资月份'].unique())}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("Processing failed. Please check the error message above.")
        raise

if __name__ == "__main__":
    main()
