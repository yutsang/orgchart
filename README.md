# Organizational Chart Processing System

A comprehensive Python script for processing monthly organizational data with automated promotion/demotion logic, supervisor relationship management, and hierarchical structure analysis.

## 📋 Overview

This system processes organizational chart data from Excel files and applies complex business rules including:

- **Rank Remapping**: Automatic conversion of legacy ranks to new hierarchy
- **Performance Assessment**: Multi-month performance evaluation windows
- **Promotion/Demotion Logic**: Configurable rules-based career progression
- **Supervisor Management**: Dynamic relationship updates based on organizational layers
- **Employee Lifecycle**: New hire and resignation detection
- **Monthly Snapshots**: Complete organizational structure tracking

## 🚀 Features

### Core Functionality
- ✅ **Rank Hierarchy Management**: AD2 → AD1 → AD0 → BM3 → BM2 → BM1 → BM0 → AS
- ✅ **Layer-Based Organization**: 3-tier organizational structure
- ✅ **Assessment Cycles**: Configurable monthly assessment periods
- ✅ **Performance Aggregation**: Multi-metric performance evaluation
- ✅ **Relationship Management**: DT/GT supervisor relationship logic
- ✅ **Status Tracking**: Employee lifecycle management
- ✅ **Data Export**: Comprehensive Excel output with UTF-8 encoding

### Advanced Features
- 🔧 **Modular Promotion Rules**: Extensible rule engine for complex promotion logic
- 📊 **Performance Metrics**: Configurable KPI thresholds and calculations
- 🎯 **Custom Assessment Windows**: Flexible evaluation periods
- 🔄 **Supervisor Cascade**: Automatic hierarchy adjustment
- 📈 **Historical Tracking**: Monthly snapshot preservation

## 📦 Installation

### Prerequisites
- Python 3.10 or higher
- pandas library

### Setup
```bash
# Clone or download the repository
git clone <repository-url>
cd orgchart

# Install dependencies
pip install pandas openpyxl

# Verify installation
python process_orgchart.py --help
```

## 🔧 Configuration

### File Structure
```
orgchart/
├── process_orgchart.py     # Main processing script
├── SelfProd_2024.xlsx      # Source data file
├── README.md               # This file
└── SelfProd_2024_processed.xlsx  # Generated output
```

### Source Data Requirements

The source Excel file must contain a "Data" sheet with these columns:

#### Required Columns
- `薪资月份` (YYYYMM format): Payroll month
- `营销员代码` (string): Agent code
- `当前职级` (string): Current rank
- `直属主管代码` (string): Direct supervisor code
- `上级主管代码` (string): Upper supervisor code
- `上级主管职级` (string): Upper supervisor rank
- `育成主管代码` (string): Mentor supervisor code
- `AD代码` (string): AD code

#### Performance Columns
- `承保FYC`: Underwriting FYC (Individual performance)
- `个险折算后FYC`: Individual insurance converted FYC
- `直辖FYC`: Direct team FYC (Direct subordinates)
- `所辖FYC`: Group FYC (All subordinates including indirect)
- `续保率`: Renewal rate
- `新单件数`: New policy count
- `直辖人力`: Direct team size (Number of direct subordinates)
- `所辖人力`: Total team size (All subordinates including indirect)
- `是否MDRT`: MDRT status

### Configuration Constants

Edit the top of `process_orgchart.py` to customize:

```python
# File paths
SRC_FILE = "SelfProd_2024.xlsx"
DST_FILE = "SelfProd_2024_processed.xlsx"

# Assessment configuration
PROMOTION_WINDOW = 6  # Months for performance evaluation
ASSESS_MONTHS = {202407, 202410}  # Assessment months

# Performance thresholds
DEFAULT_PROMOTION_THRESHOLD = 250000  # 承保FYC累计
DEFAULT_MAINTAIN_THRESHOLD = 120000   # 承保FYC累计
```

## 📋 End-to-End Processing Methodology

### Overview
The system processes organizational data chronologically from January (202401) through December (202412), building a complete historical view of organizational changes, promotions, and relationship updates.

### Detailed Processing Flow

#### Phase 1: Data Loading and Initialization
```
1. Load source data from SelfProd_2024.xlsx
2. Extract all unique months from 薪资月份 column
3. Sort months chronologically: [202401, 202402, ..., 202412]
4. Load promotion/demotion rules from Excel sheets (if available)
5. Initialize tracking variables for cross-month analysis
```

#### Phase 2: Monthly Processing Loop
**For each month in chronological order:**

##### Step 2.1: Data Extraction and Preparation
```
Current Month: 202401 → 202402 → ... → 202412

For month = 202401:
  ├── Extract all agents active in 202401
  ├── Apply rank remapping (AD→AD1, SBM→BM3, BM→BM1, AS→AS)
  ├── Calculate organizational layers for each rank
  └── Prepare data for assessment
```

##### Step 2.2: Assessment Processing (July & October Only)
```
If month ∈ {202407, 202410}:
  ├── Define Assessment Window
  │   ├── 202407 Assessment: Months [202402, 202403, 202404, 202405, 202406, 202407]
  │   └── 202410 Assessment: Months [202405, 202406, 202407, 202408, 202409, 202410]
  │
  ├── Aggregate Performance Metrics
  │   ├── Sum: 承保FYC, 个险折算后FYC, 直辖FYC, 所辖FYC, 新单件数
  │   └── Average: 续保率, 直辖人力, 所辖人力
  │
  ├── Apply Promotion Rules (Priority Order)
  │   ├── Priority 5: Grade-Specific Rules (CA→AS, AS→BM0/BM1, etc.)
  │   ├── Priority 4: Excel-Based Custom Rules
  │   ├── Priority 3: MDRT Achievement Rules
  │   ├── Priority 2: Composite Performance Rules
  │   ├── Priority 1: Basic FYC Rules
  │   └── Priority 0: Default Fallback Rules
  │
  └── Update Ranks
      ├── 晋升 (Promotion): Move up one rank level
      ├── 维持 (Maintain): Keep current rank
      └── 降级 (Demotion): Move down one rank level
```

##### Step 2.3: Employee Lifecycle Tracking
```
Compare with Previous Month (202401 vs 202312, 202402 vs 202401, etc.):
  ├── New Hires Detection
  │   ├── Agents present in current month but not in previous
  │   └── Mark as 员工状态 = "新增"
  │
  ├── Resignation Detection  
  │   ├── Agents present in previous month but not in current
  │   └── Mark as 员工状态 = "离职" (rarely appears in current data)
  │
  └── Active Employees
      └── Mark as 员工状态 = "在职"
```

##### Step 2.4: Supervisor Relationship Management
```
Layer-Based Relationship Updates:
  ├── Check Agent-Supervisor Layer Conflict
  │   ├── If agent.layer == supervisor.layer:
  │   │   ├── Move agent's supervisor up one level
  │   │   └── Store original supervisor as 育成主管代码
  │   └── Otherwise: Keep existing relationships
  │
  ├── Determine Relationship Types
  │   ├── DT (Direct Team): For ranks ≤ AS (Layer 3)
  │   └── GT (Group Team): For ranks > AS (Layers 1-2)
  │
  └── Force Business Manager Rules
      ├── Ranks [AD0, BM3, BM2, BM1] always use DT
      └── Override automatic relationship determination
```

##### Step 2.5: Output Column Generation
```
Create Hierarchical Structure Columns:
  ├── 总监代码 = AD代码
  ├── 总监职级 = "AD1" (default)
  ├── 总监关系 = "GT"
  ├── 业务经理代码 = 直属主管代码
  ├── 业务经理职级 = 直属主管职级
  ├── 业务经理关系 = calculated relationship type
  ├── 业务主任代码 = 上级主管代码
  ├── 业务主任职级 = 上级主管职级
  ├── 业务主任关系 = calculated relationship type
  ├── 一代育成人 = 育成主管代码
  └── 二代育成人 = "" (placeholder)
```

##### Step 2.6: Monthly Snapshot Storage
```
Store Complete Monthly State:
  ├── All original columns from source data
  ├── All calculated performance metrics
  ├── Updated ranks and relationships
  ├── Employee status indicators
  └── Hierarchical structure mappings
```

#### Phase 3: Historical Integration
```
After Processing All 12 Months:
  ├── Combine monthly snapshots vertically
  ├── Preserve chronological order
  ├── Maintain all historical state changes
  └── Format final output structure
```

#### Phase 4: Output Generation
```
Final Dataset Structure:
  ├── Required Output Columns (first 13 columns)
  │   ├── 营销员代码, 上级主管代码, 总监代码, etc.
  │   └── Standardized order for consistency
  │
  ├── Original Source Columns
  │   ├── All columns from source Excel file
  │   └── Preserves original data integrity
  │
  ├── Calculated Columns
  │   ├── RANK_LAYER: Organizational layer (1-3)
  │   ├── 升降级标记: Promotion decision (晋升/维持/降级)
  │   ├── 员工状态: Employee status (新增/在职/离职)
  │   └── Performance aggregations (*_累计 columns)
  │
  └── Export to SelfProd_2024_processed.xlsx
```

### Key Processing Characteristics

#### 1. **Chronological Dependency**
```
Month N Processing Depends On:
  ├── Month N-1: For employee lifecycle tracking
  ├── Months N-5 to N: For performance assessment (6-month window)
  └── All previous months: For historical context
```

#### 2. **State Preservation**
```
Each Month Maintains:
  ├── Complete organizational snapshot
  ├── All promotion/demotion decisions
  ├── Supervisor relationship changes
  └── Employee status transitions
```

#### 3. **Assessment Timing**
```
Assessment Months (202407, 202410):
  ├── Trigger comprehensive performance evaluation
  ├── Apply all promotion/demotion rules
  ├── Update organizational structure
  └── Cascade supervisor relationships
```

#### 4. **Data Integrity**
```
Quality Assurance:
  ├── Preserve all original data columns
  ├── Maintain referential integrity in supervisor codes
  ├── Validate promotion rule applications
  └── Ensure chronological consistency
```

### Example Processing Sequence

```
Processing Timeline:
202401 → 202402 → 202403 → 202404 → 202405 → 202406 → 
202407 (Assessment) → 202408 → 202409 → 202410 (Assessment) → 202411 → 202412

Month 202401:
  ├── 1,200 agents processed
  ├── 45 new hires detected
  ├── No assessment (regular month)
  └── Snapshot stored

Month 202407 (Assessment):
  ├── 1,350 agents processed
  ├── 15 new hires detected
  ├── Performance window: [202402-202407]
  ├── 89 promotions, 12 demotions, 1,249 maintained
  ├── 156 supervisor relationships updated
  └── Snapshot stored

Final Output:
  ├── 15,800 total records (12 months × ~1,300 agents)
  ├── Complete historical progression
  ├── All promotion decisions tracked
  └── Comprehensive organizational evolution
```

## 🎯 Usage

### Basic Usage
```bash
python process_orgchart.py
```

### Advanced Usage with Custom Rules

#### 1. Create Promotion Rules Sheet
Add a "PromotionRules" sheet to your Excel file with columns:
- `当前职级`: Current rank
- `目标职级`: Target rank
- `承保FYC要求`: FYC requirement
- `续保率要求`: Renewal rate requirement
- `新单件数要求`: New policy count requirement

#### 2. Create Demotion Rules Sheet
Add a "DemotionRules" sheet with similar structure for demotion criteria.

#### 3. Run Processing
The script automatically detects and uses these rule sheets if present.

## 📊 Business Rules

### Rank Remapping (职级套转)
```
AD  → AD1
SBM → BM3
BM  → BM1
AS  → AS (unchanged)
```

### Layer Definition
- **Layer 1**: AD2, AD1 (Senior Management)
- **Layer 2**: AD0, BM3, BM2, BM1 (Middle Management)  
- **Layer 3**: BM0, AS (Front-line)

### Assessment Logic

#### Assessment Months
- **July (202407)**: Mid-year assessment
- **October (202410)**: Year-end assessment

#### Performance Window
- Default: 6 months preceding assessment month
- Configurable via `PROMOTION_WINDOW` constant

#### Grade-Specific Promotion Requirements

The system implements detailed, grade-specific promotion requirements based on your organizational standards:

#### 晋升要求 (Promotion Requirements)

| Current Rank | Target Rank | Time (Months) | Individual FYC | Direct FYC | Group FYC | Renewal Rate | Team Size |
|--------------|-------------|---------------|----------------|------------|-----------|--------------|-----------|
| CA | AS | 3 | 9,000 | - | - | 85% | - |
| AS | BM0 | 6 | 18,000 | 36,000 | - | 85% | 4 |
| AS | BM1* | 6 | 18,000 | 90,000 | - | 85% | 6 |
| BM0 | BM1 | 6 | 12,000 | 63,000 | - | 85% | 4 |
| BM1 | BM2 | 12 | 12,000 | 105,000 | - | 85% | 8 |
| BM2 | BM3 | 12 | 12,000 | 175,000 | - | 85% | 14 |
| BM2 | AD0* | 12 | 12,000 | 112,500 | 300,000 | 85% | 21 |
| BM3 | AD0 | 12 | 12,000 | 112,500 | 300,000 | 85% | 21 |
| AD0 | AD1 | 12 | 12,000 | 135,000 | 450,000 | 85% | 38 |
| AD1 | AD2 | 12 | - | - | 2,000,000 | - | 80 |

*Alternative promotion paths for exceptional performance

#### 维持要求 (Maintenance Requirements)

| Current Rank | Individual FYC | Direct FYC | Group FYC | Renewal Rate | Team Size |
|--------------|----------------|------------|-----------|--------------|-----------|
| AS | 9,000 | 15,000 | - | 85% | 3 |
| BM0 | 12,000 | 25,200 | - | 85% | 2 |
| BM1 | 12,000 | 63,000 | - | 85% | 4 |
| BM2 | 12,000 | 105,000 | - | 85% | 8 |
| BM3 | 12,000 | 175,000 | - | 85% | 14 |
| AD0 | 12,000 | 112,500 | 300,000 | 85% | 21 |
| AD1 | 12,000 | 135,000 | 450,000 | 85% | 38 |
| AD2 | - | - | 2,000,000 | - | 80 |

#### Performance Metrics Definitions

- **Individual FYC** (承保FYC): Personal underwriting FYC
- **Direct FYC** (直辖FYC): Direct team FYC (个险折算后FYC)
- **Group FYC** (所辖FYC): Total group FYC including all subordinates
- **Renewal Rate** (续保率): Policy renewal percentage
- **Team Size** (直辖人力/所辖人力): Number of direct/total subordinates

### Fallback Decision Matrix (when grade-specific rules don't apply)
| Performance Level | 承保FYC累计 | Decision |
|------------------|-------------|----------|
| Excellent | ≥ 250,000 | 晋升 (Promotion) |
| Good | 120,000 - 249,999 | 维持 (Maintain) |
| Needs Improvement | < 120,000 | 降级 (Demotion) |

### Supervisor Relationships

#### Relationship Types
- **DT (Direct Team)**: For ranks ≤ AS
- **GT (Group Team)**: For ranks > AS

#### Layer-Based Adjustments
When agent and supervisor are in the same layer:
1. Move agent's supervisor up one level
2. Store original supervisor as mentor (育成主管代码)

## 📈 Output Format

The processed file contains all original columns plus:

### Required Output Columns
1. `营销员代码` - Agent code
2. `上级主管代码` - Upper supervisor code
3. `总监代码` - Director code
4. `总监职级` - Director rank
5. `总监关系` - Director relationship
6. `业务经理代码` - Business manager code
7. `业务经理职级` - Business manager rank
8. `业务经理关系` - Business manager relationship
9. `业务主任代码` - Business supervisor code
10. `业务主任职级` - Business supervisor rank
11. `业务主任关系` - Business supervisor relationship
12. `一代育成人` - First-generation mentor
13. `二代育成人` - Second-generation mentor

### Additional Columns
- `员工状态`: Employee status (新增/在职/离职)
- `升降级标记`: Promotion/demotion flag
- `RANK_LAYER`: Calculated organizational layer
- Performance aggregation columns with `_累计` suffix

## 🔍 Troubleshooting

### Common Issues

#### File Not Found
```
FileNotFoundError: Source file not found: SelfProd_2024.xlsx
```
**Solution**: Ensure the source Excel file exists in the same directory as the script.

#### Missing Columns
```
KeyError: '营销员代码'
```
**Solution**: Verify all required columns exist in the "Data" sheet.

#### Memory Issues
```
MemoryError: Unable to allocate array
```
**Solution**: Process data in smaller chunks or increase system memory.

### Debug Mode
Add debug prints by modifying the script:
```python
# Add after line 1
DEBUG = True

# Use throughout code
if DEBUG:
    print(f"Debug: Processing {len(df)} records")
```

## 🛠️ Customization

### Adding New Performance Metrics

1. **Update Performance Columns**:
```python
PERFORMANCE_COLUMNS = [
    '续保率', '承保FYC', '个险折算后FYC', '新单件数', '是否MDRT',
    'your_new_metric'  # Add here
]
```

2. **Update Aggregation Rules**:
```python
agg_rules = {
    '承保FYC': 'sum',
    '个险折算后FYC': 'sum',
    '续保率': 'mean',
    '新单件数': 'sum',
    'your_new_metric': 'mean'  # Add here
}
```

### Custom Promotion Logic

Create a custom promotion rule function:
```python
def custom_promotion_logic(performance_data: Dict, current_rank: str) -> str:
    """
    Custom promotion decision logic.
    
    Args:
        performance_data: Dictionary of performance metrics
        current_rank: Current rank of the agent
        
    Returns:
        Decision: '晋升', '维持', or '降级'
    """
    # Your custom logic here
    fyc = performance_data.get('承保FYC', 0)
    renewal_rate = performance_data.get('续保率', 0)
    
    if fyc >= 300000 and renewal_rate >= 0.9:
        return '晋升'
    elif fyc >= 150000 and renewal_rate >= 0.8:
        return '维持'
    else:
        return '降级'
```

## 📝 Change Log

### Version 1.0.0
- Initial release with core functionality
- Basic promotion/demotion logic
- Supervisor relationship management
- Monthly snapshot processing

### Planned Features
- [ ] Web-based configuration interface
- [ ] Advanced analytics dashboard
- [ ] Multi-company support
- [ ] API endpoints for integration
- [ ] Real-time processing capabilities

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Create Pull Request

## 📞 Support

For questions or issues:
1. Check the troubleshooting section above
2. Review the configuration settings
3. Verify your data format matches requirements
4. Create an issue with detailed error information

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: This system processes sensitive organizational data. Ensure proper data security measures and compliance with your organization's data handling policies.
