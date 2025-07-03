# Daily Distribution Bug Fixes

## Issues Reported

The user identified two critical bugs in the SPSS daily distribution logic:

### Bug 1: Theory ID Assignment Issue
- **Problem**: Control group correctly got `SC00000001.1`, but target groups incorrectly got `SC00000001` instead of `SC00000001.2` and `SC00000001.3`
- **Root Cause**: All groups were using the same base theory ID from the campaign record

### Bug 2: Additional Fields Issue  
- **Problem**: All groups got the same tab1-tab5 values instead of group-specific values
- **Root Cause**: All groups were receiving `additional_fields=None`

## Root Cause Analysis

The daily distribution process in `database.py` had fundamental differences from the normal stratification process:

### Normal Stratification (Working Correctly)
- Group A gets unique theory ID: `SC00000001.1`
- Group B gets unique theory ID: `SC00000001.2`  
- Group C gets unique theory ID: `SC00000001.3`
- Each group gets its own `additional_fields` (tab1-tab5)

### Daily Distribution (Broken)
- All groups used the same base `theory_id` from campaign record
- All groups received `additional_fields=None`

## Fixes Applied

### 1. Updated `distribute_users_to_campaigns()` Function

**Before:**
```python
group_distributions[group_letter] = group_users
```

**After:**
```python
# Extract base campaign ID (e.g., "SC00000001" from "SC00000001.1")
base_campaign_id = campaign["theory_id"]
if "." in base_campaign_id:
    base_campaign_id = base_campaign_id.split(".")[0]

# Create unique theory ID for each group
group_theory_id = f"{base_campaign_id}.{group_idx + 1}"

# Create group-specific additional fields
group_additional_fields = {
    'tab1': f"Daily Distribution Group {group_letter}",
    'tab2': f"Generated on {datetime.now().strftime('%Y-%m-%d')}",
    'tab3': f"Count: {len(group_users)}",
    'tab4': f"Campaign: {campaign['theory_name']}",
    'tab5': f"Group Type: {'Control' if group_letter == 'A' else 'Target'}"
}

group_distributions[group_letter] = {
    "users": group_users,
    "theory_id": group_theory_id,
    "additional_fields": group_additional_fields
}
```

### 2. Updated `insert_daily_distributed_users()` Function

**Before:**
```python
for group_letter, group_users in groups.items():
    # Used same theory_id for all groups
    result = insert_control_group(
        theory_id=theory_id,  # Same for all!
        additional_fields=None  # None for all!
    )
```

**After:**
```python
for group_letter, group_data in groups.items():
    group_users = group_data["users"]
    group_theory_id = group_data["theory_id"]  # Unique per group
    group_additional_fields = group_data["additional_fields"]  # Group-specific
    
    result = insert_control_group(
        theory_id=group_theory_id,  # Use group-specific theory ID
        additional_fields=group_additional_fields  # Use group-specific fields
    )
```

### 3. Updated Test Logic

Updated `test_daily_distribution.py` to handle the new data structure and display the correct theory IDs and additional fields.

## Expected Results After Fix

### Theory ID Distribution
- **Control Group (A)**: Gets `SC00000001.1`
- **Target Group B**: Gets `SC00000001.2` 
- **Target Group C**: Gets `SC00000001.3`
- **Target Group D**: Gets `SC00000001.4`
- **Target Group E**: Gets `SC00000001.5`

### Additional Fields (tab1-tab5)
Each group now gets unique values:
- **tab1**: "Daily Distribution Group A", "Daily Distribution Group B", etc.
- **tab2**: Generation date (e.g., "Generated on 2024-01-15")
- **tab3**: User count for that group (e.g., "Count: 500")
- **tab4**: Campaign name
- **tab5**: Group type ("Control" for Group A, "Target" for others)

### Database Distribution
- **SC_local_control**: Only receives users with theory_id ending in `.1` (Group A)
- **SC_local_target**: Receives users with theory_ids ending in `.2`, `.3`, `.4`, `.5` (Groups B, C, D, E)
- **SPSS SC_theory_users**: Receives duplicates of target groups (.2, .3, .4, .5) only

## Testing

Run the test script to verify fixes:
```bash
python test_daily_distribution.py
```

The test will now show:
- Unique theory IDs for each group
- Group-specific additional fields
- Correct distribution logic

## Verification Queries

After running daily distribution, verify the fixes with these queries:

```sql
-- Check control group has only .1 theory IDs
SELECT DISTINCT THEORY_ID FROM SC_local_control 
WHERE THEORY_ID LIKE 'SC%'
ORDER BY THEORY_ID;

-- Check target group has .2, .3, .4, .5 theory IDs
SELECT DISTINCT THEORY_ID FROM SC_local_target 
WHERE THEORY_ID LIKE 'SC%'
ORDER BY THEORY_ID;

-- Check SPSS has only target groups (.2, .3, .4, .5)
SELECT DISTINCT THEORY_ID FROM SPSS_USER_DRACRM.SC_theory_users 
WHERE THEORY_ID LIKE 'SC%'
ORDER BY THEORY_ID;

-- Check tab fields are unique per group
SELECT THEORY_ID, TAB1, TAB2, TAB5 FROM SC_local_control 
WHERE THEORY_ID LIKE 'SC%'
UNION ALL
SELECT THEORY_ID, TAB1, TAB2, TAB5 FROM SC_local_target 
WHERE THEORY_ID LIKE 'SC%'
ORDER BY THEORY_ID;
```

## Files Modified

1. **database.py**: Fixed core distribution and insertion logic
2. **test_daily_distribution.py**: Updated test to handle new data structure
3. **DAILY_DISTRIBUTION_FIXES.md**: This documentation file

The fixes ensure that each group gets its proper theory ID and unique additional field values, resolving both reported bugs. 