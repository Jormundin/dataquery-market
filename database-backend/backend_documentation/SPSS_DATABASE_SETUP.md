# SPSS Database Setup for Dual Database Architecture

## Overview

The SoftCollection system now supports **dual database architecture** where:

- **DSSB_APP Database**: Primary database containing SC_local_control and SC_local_target tables
- **SPSS Database**: Secondary database containing SC_theory_users table (identical structure to SC_local_target)

When target groups are created during stratification, the data is automatically duplicated to both databases.

## Database Flow

```
Stratification Process
├── Control Group (A) → DSSB_APP.SC_local_control
└── Target Groups (B,C,D,E) → DSSB_APP.SC_local_target + SPSS.SC_theory_users
```

## Required Environment Variables

### Existing DSSB_APP Variables (unchanged)
```bash
ORACLE_HOST=your_dssb_host
ORACLE_PORT=1521
ORACLE_SID=your_dssb_sid
ORACLE_USER=your_dssb_user
ORACLE_PASSWORD=your_dssb_password
```

### New SPSS Database Variables
```bash
SPSS_ORACLE_HOST=your_spss_host
SPSS_ORACLE_PORT=1521
SPSS_ORACLE_SID=your_spss_sid
SPSS_ORACLE_USER=your_spss_user
SPSS_ORACLE_PASSWORD=your_spss_password
```

## SC_theory_users Table Structure

The SC_theory_users table in the SPSS database should have the same structure as SC_local_target:

```sql
CREATE TABLE SC_theory_users (
    IIN VARCHAR2(12) NOT NULL,
    THEORY_ID VARCHAR2(50) NOT NULL,
    date_start DATE,
    date_end DATE,
    insert_datetime DATE DEFAULT SYSDATE,
    tab1 VARCHAR2(4000),
    tab2 VARCHAR2(4000),
    tab3 VARCHAR2(4000),
    tab4 VARCHAR2(4000),
    tab5 VARCHAR2(4000)
);

-- Recommended indexes for performance
CREATE INDEX idx_sc_theory_users_theory_id ON SC_theory_users(THEORY_ID);
CREATE INDEX idx_sc_theory_users_iin ON SC_theory_users(IIN);
CREATE INDEX idx_sc_theory_users_dates ON SC_theory_users(date_start, date_end);
```

## Setup Instructions

1. **Configure Environment Variables**
   - Add the SPSS database variables to your environment
   - Ensure both database connections are accessible from your application server

2. **Test Database Connections**
   ```bash
   # Test individual connections
   curl -X POST "http://localhost:8000/databases/test-connection" \
        -H "Authorization: Bearer YOUR_TOKEN"
   
   # Test both connections
   curl -X POST "http://localhost:8000/databases/test-all-connections" \
        -H "Authorization: Bearer YOUR_TOKEN"
   ```

3. **Verify Table Structure**
   - Ensure SC_theory_users table exists in SPSS database
   - Verify column names and types match SC_local_target

## Operational Behavior

### Success Scenarios
- **Both databases succeed**: Data inserted into both DSSB_APP.SC_local_target and SPSS.SC_theory_users
- **DSSB_APP succeeds, SPSS fails**: Stratification continues (SPSS is considered optional)

### Failure Scenarios
- **DSSB_APP fails**: Entire stratification process fails (primary database required)
- **SPSS fails**: Warning logged, but stratification completes successfully

### Response Structure
```json
{
  "success": true,
  "inserted_count": 1000,
  "message": "Successfully inserted into both databases: DSSB_APP: 1000 users inserted into SC_local_target, SPSS: 1000 users inserted into SC_theory_users",
  "detailed_results": {
    "dssb_app": {
      "success": true,
      "inserted_count": 1000,
      "message": "Inserted 1000 users into DSSB_APP SC_local_target"
    },
    "spss": {
      "success": true,
      "inserted_count": 1000,
      "message": "Inserted 1000 users into SPSS SC_theory_users table"
    },
    "overall_success": true,
    "total_inserted": 2000,
    "messages": [
      "DSSB_APP: 1000 users inserted into SC_local_target",
      "SPSS: 1000 users inserted into SC_theory_users"
    ]
  }
}
```

## Troubleshooting

### Connection Issues
1. **SPSS Connection Failed**
   - Check SPSS_ORACLE_* environment variables
   - Verify network connectivity to SPSS database
   - Confirm SPSS user has INSERT privileges on SC_theory_users

2. **Table Not Found**
   - Ensure SC_theory_users table exists in SPSS database
   - Verify table structure matches expected format

3. **Permission Issues**
   - Confirm SPSS user has required privileges:
     ```sql
     GRANT INSERT ON SC_theory_users TO your_spss_user;
     GRANT SELECT ON SC_theory_users TO your_spss_user;
     ```

### Monitoring
- Check application logs for SPSS insertion results
- Use test endpoints to verify connectivity
- Monitor both databases for data consistency

## Migration Notes

- **Backward Compatibility**: Existing stratification processes continue to work
- **No Code Changes Required**: Frontend remains unchanged
- **Optional SPSS**: System functions normally if SPSS database is unavailable
- **Data Integrity**: Primary operations never fail due to SPSS issues 