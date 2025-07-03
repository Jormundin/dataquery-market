# Daily Distribution - Corrected Implementation

## ❌ Previous Misunderstanding

I initially misunderstood the task and implemented:
- Creating NEW theory IDs (1.4, 1.5, 1.6) for daily distribution
- Filling tab1-tab5 with generic "Daily Distribution" text
- Essentially creating new campaign versions instead of adding to existing ones

## ✅ Correct Understanding

Daily distribution should:
1. **Find active campaigns** (base IDs like `SC00000001`)
2. **Find EXISTING groups** that were already created during initial stratification
3. **Get existing tab1-tab5 values** from those groups
4. **Distribute new COUNT_DAY=5 users INTO existing groups** with the same tab values
5. **No new theory IDs** - just add users to existing groups

## Example Scenario

### Initial Campaign Setup (via Stratification)
When a campaign `SC00000001` was initially created with stratification:
- `SC00000001.1` (Group A - Control) → `SC_local_control` with `tab1="control settings", tab2=NULL, tab3=NULL, tab4=NULL, tab5=NULL`
- `SC00000001.2` (Group B - Target) → `SC_local_target` + SPSS with `tab1="call before lunch", tab2="weekdays", tab3=NULL, tab4=NULL, tab5=NULL`  
- `SC00000001.3` (Group C - Target) → `SC_local_target` + SPSS with `tab1="call after lunch", tab2="weekends", tab3=NULL, tab4=NULL, tab5=NULL`

### Daily Distribution Process
When daily distribution runs and finds 500 new COUNT_DAY=5 users:
1. **Find existing groups**: `SC00000001.1`, `SC00000001.2`, `SC00000001.3`
2. **Get their tab values**: from existing records in SC_local_control and SC_local_target
3. **Distribute 500 users** proportionally among these 3 existing groups (167, 167, 166)
4. **Insert with same tab values**:
   - 167 users → `SC00000001.1` in `SC_local_control` with `tab1="control settings"`
   - 167 users → `SC00000001.2` in `SC_local_target` + SPSS with `tab1="call before lunch", tab2="weekdays"`
   - 166 users → `SC00000001.3` in `SC_local_target` + SPSS with `tab1="call after lunch", tab2="weekends"`

## New Implementation

### 1. `get_existing_campaign_groups(base_campaign_id)`
```python
def get_existing_campaign_groups(base_campaign_id):
    """Get existing groups for a campaign and their tab field values"""
    # Query SC_local_control for groups like SC00000001.1, SC00000001.2, etc.
    # Query SC_local_target for groups like SC00000001.1, SC00000001.2, etc.
    # Return: {
    #   "SC00000001.1": {"table": "SC_local_control", "group_type": "control", "tab_values": {...}},
    #   "SC00000001.2": {"table": "SC_local_target", "group_type": "target", "tab_values": {...}},
    #   ...
    # }
```

### 2. Updated `distribute_users_to_campaigns()`
```python
def distribute_users_to_campaigns(iin_values, campaigns):
    """Distribute users equally among active campaigns into their EXISTING groups"""
    # For each campaign:
    #   1. Extract base_campaign_id (SC00000001)
    #   2. Find existing groups using get_existing_campaign_groups()
    #   3. Distribute users among those existing groups
    #   4. Preserve existing tab1-tab5 values for each group
```

### 3. Updated `insert_daily_distributed_users()`
```python
def insert_daily_distributed_users(distributions):
    """Insert distributed users into existing groups with their original tab values"""
    # For each group:
    #   - Use existing theory_id (not new one)
    #   - Use existing tab1-tab5 values (not generic ones)
    #   - Insert into appropriate table based on group_type
```

## Key Differences from Previous Implementation

| Aspect | ❌ Previous (Wrong) | ✅ Current (Correct) |
|--------|-------------------|-------------------|
| **Theory IDs** | Create new: `1.4`, `1.5`, `1.6` | Use existing: `1.1`, `1.2`, `1.3` |
| **Tab Values** | Generic: "Daily Distribution Group A" | Existing: "call before lunch" |
| **Group Discovery** | Hardcoded 5 groups (A,B,C,D,E) | Query database for existing groups |
| **Purpose** | Create new campaign versions | Add users to existing campaigns |

## Database Queries Used

### Find Existing Control Groups
```sql
SELECT DISTINCT THEORY_ID, tab1, tab2, tab3, tab4, tab5
FROM SC_local_control 
WHERE THEORY_ID LIKE 'SC00000001%'
```

### Find Existing Target Groups  
```sql
SELECT DISTINCT THEORY_ID, tab1, tab2, tab3, tab4, tab5
FROM SC_local_target 
WHERE THEORY_ID LIKE 'SC00000001%'
```

## Expected Results

### Before Daily Distribution
```
SC_local_control:
- SC00000001.1: 1000 users, tab1="control settings"

SC_local_target:  
- SC00000001.2: 1000 users, tab1="call before lunch", tab2="weekdays"
- SC00000001.3: 1000 users, tab1="call after lunch", tab2="weekends"

SPSS.SC_theory_users:
- SC00000001.2: 1000 users, tab1="call before lunch", tab2="weekdays"  
- SC00000001.3: 1000 users, tab1="call after lunch", tab2="weekends"
```

### After Daily Distribution (500 new users)
```
SC_local_control:
- SC00000001.1: 1167 users, tab1="control settings"

SC_local_target:
- SC00000001.2: 1167 users, tab1="call before lunch", tab2="weekdays"
- SC00000001.3: 1166 users, tab1="call after lunch", tab2="weekends"

SPSS.SC_theory_users:
- SC00000001.2: 1167 users, tab1="call before lunch", tab2="weekdays"
- SC00000001.3: 1166 users, tab1="call after lunch", tab2="weekends"
```

## Testing

Run the updated test:
```bash
python test_daily_distribution.py
```

The test will now show:
- Finding existing groups from database
- Using existing theory IDs
- Preserving existing tab1-tab5 values
- Distributing to existing groups instead of creating new ones

## Files Modified

1. **database.py**: 
   - Added `get_existing_campaign_groups()`
   - Completely rewrote `distribute_users_to_campaigns()`
   - Updated `insert_daily_distributed_users()`

2. **test_daily_distribution.py**: Updated to handle new data structure

3. **DAILY_DISTRIBUTION_FIXES_CORRECTED.md**: This corrected documentation

The implementation now correctly adds users to existing campaign groups while preserving their original tab field configurations. 