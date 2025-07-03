# Database Connections Setup

## Overview

This document describes the database connection setup for the DataQuery Pro application. The system now supports 4 different Oracle database connections to handle various aspects of the campaign management functionality migrated from the original market.py system.

## Database Connections

### 1. DSSB_APP (Primary Database)
**Purpose**: Main corporate database for data analysis and queries
**Environment Variables**:
```bash
ORACLE_HOST=your-dssb-app-host
ORACLE_PORT=1521
ORACLE_SID=your-dssb-app-sid
ORACLE_USER=your-dssb-app-username
ORACLE_PASSWORD=your-dssb-app-password
```

### 2. SPSS (Analytics Database)  
**Purpose**: SPSS analytics database
**Environment Variables**:
```bash
SPSS_ORACLE_HOST=your-spss-host
SPSS_ORACLE_PORT=1521
SPSS_ORACLE_SID=your-spss-sid
SPSS_ORACLE_USER=your-spss-username
SPSS_ORACLE_PASSWORD=your-spss-password
```

### 3. DSSB_OCDS (Campaign Management)
**Purpose**: Campaign management database for RB1/RB3 campaigns
**Environment Variables**:
```bash
DSSB_OCDS_ORACLE_HOST=your-dssb-ocds-host
DSSB_OCDS_ORACLE_PORT=1521
DSSB_OCDS_ORACLE_SID=your-dssb-ocds-sid
DSSB_OCDS_ORACLE_USER=your-dssb-ocds-username
DSSB_OCDS_ORACLE_PASSWORD=your-dssb-ocds-password
```

### 4. ED_OCDS (Campaign Tracking)
**Purpose**: Campaign execution tracking and monitoring
**Environment Variables**:
```bash
ED_OCDS_ORACLE_HOST=your-ed-ocds-host
ED_OCDS_ORACLE_PORT=1521
ED_OCDS_ORACLE_SID=your-ed-ocds-sid
ED_OCDS_ORACLE_USER=your-ed-ocds-username
ED_OCDS_ORACLE_PASSWORD=your-ed-ocds-password
```

## Database Usage in Market.py Migration

### DSSB_APP / get_connection()
- Used for: Main data queries, feature store access
- Tables: `dssb_app.rb_feature_store`, `dssb_dm.rb_clients`
- Functionality: Customer data, segmentation, filtering

### DSSB_OCDS / get_connection_DSSB_OCDS()  
- Used for: Campaign dictionary, target/control groups
- Tables: `dssb_ocds.mb01_camp_dict`, `dssb_ocds.mb22_local_target`, `dssb_ocds.mb21_local_control`
- Functionality: Campaign metadata, group management

### SPSS / get_connection_SPSS()
- Used for: SPSS analytics AND campaign user assignments  
- Tables: SPSS analytics tables, `spss_ocds.fd_rb2_campaigns_users`, `off_limit_campaigns_users`
- Functionality: Analytics, user targeting, campaign execution

### ED_OCDS / get_connection_ED_OCDS()
- Used for: Historical campaign tracking
- Tables: `dds.mc_campaign_fl`
- Functionality: Campaign history, performance tracking

## API Endpoints

### Connection Testing
- `POST /databases/test-connection` - Test DSSB_APP connection
- `POST /databases/test-spss-connection` - Test SPSS connection  
- `POST /databases/test-dssb-ocds-connection` - Test DSSB_OCDS connection
- `POST /databases/test-ed-ocds-connection` - Test ED_OCDS connection
- `POST /databases/test-all-connections` - Test all connections

### Enhanced Response Format
The `/databases/test-all-connections` endpoint now returns:
```json
{
  "dssb_app": {"status": "success", "message": "...", "connected": true},
  "spss": {"status": "success", "message": "...", "connected": true},
  "dssb_ocds": {"status": "success", "message": "...", "connected": true},
  "ed_ocds": {"status": "success", "message": "...", "connected": true},
  "overall_status": "success",
  "message": "Все соединения успешны",
  "successful_connections": 4,
  "total_connections": 4
}
```

## Configuration Steps

1. **Copy environment template**:
   ```bash
   cp .env.example .env
   ```

2. **Update .env file** with your actual database credentials

3. **Test connections** using the API endpoints or through the frontend

4. **Verify connectivity** - All 4 connections should be successful for full functionality

## Troubleshooting

### Common Issues
1. **Missing environment variables**: Check that all required variables are set
2. **Network connectivity**: Ensure firewall allows connections to Oracle hosts
3. **Authentication**: Verify username/password combinations
4. **Oracle client**: Ensure cx_Oracle is properly installed and configured

### Error Messages
- `Missing required database environment variables`: Check .env file
- `Database connection error`: Check host/port/sid values
- `Authentication failed`: Check username/password

## Next Steps

With database connections established, you can now:
1. ✅ Test all database connectivity
2. 🔄 Add campaign management models  
3. 🔄 Implement campaign filtering functionality
4. 🔄 Add stop list management
5. 🔄 Integrate product-based targeting

## Security Notes

- Store credentials securely in .env files
- Use different passwords for each database
- Implement IP whitelisting where possible
- Regular password rotation recommended
- Monitor connection logs for suspicious activity 