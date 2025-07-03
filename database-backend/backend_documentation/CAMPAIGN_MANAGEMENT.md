# Campaign Management Service Documentation

## Overview

The Campaign Management Service is the core component of DataQuery Pro that handles RB1 and RB3 campaign creation, CAMPAIGNCODE generation, data filtering, and deployment to multiple Oracle tables. This service directly replaces the campaign functionality from the original market.py system.

## Purpose

This service provides complete campaign lifecycle management:
- **Campaign Code Generation**: Automatic CAMPAIGNCODE and XLS_OW_ID generation
- **Data Filtering**: Integration with parquet service for comprehensive data filtering
- **Multi-table Deployment**: Deployment to 4+ Oracle tables for campaign execution
- **Campaign Management**: CRUD operations for campaign metadata and user data

## Architecture

### Service Components

```
Campaign Management System/
├── CampaignCodeService          # Code generation logic
├── CampaignDataProcessor        # Data filtering integration
├── CampaignDeploymentService    # Multi-table deployment
└── CampaignService             # Main orchestration service
```

### Database Integration

**Campaign Metadata Tables:**
- `dssb_ocds.mb01_camp_dict` - RB1 campaign metadata
- `dssb_ocds.rb3_tr_campaign_dict` - RB3 campaign metadata

**User/Targeting Tables:**
- `dssb_ocds.mb22_local_target` - Campaign targeting users
- `dssb_ocds.mb21_local_control` - Control groups (stratification)
- `spss.fd_rb2_campaigns_users` - Main campaign user list
- `spss.off_limit_campaigns_users` - Off-limit tracking

## Campaign Types

### RB1 Campaigns (Standard Marketing)

**Metadata Fields:**
- Basic campaign information (name, description, dates)
- Targeting information (stream, sub_stream, target_action)
- Channel information (Push, POP-UP, SMS)
- Campaign content (texts in KZ/RU)
- Launch configuration (out_date, camp_cnt)

**Use Cases:**
- Standard push notification campaigns
- Pop-up advertising campaigns
- SMS marketing campaigns
- General marketing initiatives

### RB3 Campaigns (Bonus/Reward Marketing)

**Additional Metadata:**
- `bonus` - Bonus information
- `characteristic_json` - JSON configuration
- `xls_ow_id` - Unique XLS identifier (KKB_XXXX format)

**Use Cases:**
- Bonus reward campaigns
- Loyalty program promotions
- Special offer campaigns
- Complex targeting campaigns

## API Endpoints

### 1. Campaign Code Generation

#### Get Next RB1 Code
```http
GET /campaigns/codes/next-rb1
```

**Response:**
```json
{
  "campaign_code": "C000012345",
  "campaign_type": "RB1",
  "generated_at": "2024-01-15T10:30:00Z"
}
```

#### Get Next RB3 Codes
```http
GET /campaigns/codes/next-rb3
```

**Response:**
```json
{
  "campaign_code": "C000012346",
  "campaign_type": "RB3",
  "xls_ow_id": "KKB_0123",
  "generated_at": "2024-01-15T10:30:00Z"
}
```

### 2. Campaign Creation

#### Create Campaign
```http
POST /campaigns/create
```

**Request Body:**
```json
{
  "campaign_type": "RB1",
  "metadata": {
    "campaign_name": "Spring Marketing Campaign",
    "campaign_desc": "Promote spring products",
    "stream": "market",
    "sub_stream": "seasonal",
    "target_action": "purchase",
    "channel": "Push",
    "campaign_type": "promotion",
    "campaign_text": "Check out our spring collection!",
    "campaign_text_kz": "Көктемгі коллекциямызды қараңыз!",
    "short_desc": "Spring promo",
    "date_start": "2024-03-01",
    "date_end": "2024-03-31",
    "out_date": "2024-03-02",
    "camp_cnt": "50000"
  },
  "user_iins": ["123456789012", "987654321098", "456789123456"],
  "filter_config": {
    "blacklist_tables": ["ACRM_DW.RB_BLACK_LIST@ACRM"],
    "devices": ["android", "iOS"],
    "mau_only": true,
    "products": ["Кредитная карта"]
  },
  "deploy_options": {
    "deploy_metadata": true,
    "deploy_targeting": true,
    "deploy_users": true,
    "deploy_offlimit": true
  }
}
```

**Response:**
```json
{
  "success": true,
  "campaign_code": "C000012347",
  "campaign_type": "RB1",
  "filter_stats": {
    "initial_count": 3,
    "final_count": 2,
    "blacklist_removed": 1,
    "total_removed": 1
  },
  "deployment_result": {
    "campaign_code": "C000012347",
    "tables_updated": [
      "dssb_ocds.mb01_camp_dict",
      "dssb_ocds.mb22_local_target",
      "spss.fd_rb2_campaigns_users",
      "spss.off_limit_campaigns_users"
    ],
    "total_users": 2,
    "errors": [],
    "success": true
  },
  "message": "Successfully created RB1 campaign C000012347"
}
```

### 3. Campaign Management

#### List Campaigns
```http
GET /campaigns/list?limit=50&offset=0&campaign_type=RB1
```

**Response:**
```json
{
  "campaigns": [
    {
      "campaign_code": "C000012347",
      "campaign_type": "RB1",
      "campaign_name": "Spring Marketing Campaign",
      "stream": "market",
      "channel": "Push",
      "date_start": "2024-03-01",
      "date_end": "2024-03-31",
      "user_count": 2,
      "created_at": "2024-01-15T10:30:00Z",
      "status": "Active"
    }
  ],
  "total_count": 1,
  "rb1_count": 15,
  "rb3_count": 8
}
```

#### Get Campaign Details
```http
GET /campaigns/{campaign_code}
```

#### Delete Campaign
```http
DELETE /campaigns/{campaign_code}
```

## Data Filtering Integration

The campaign service integrates with the Parquet Data Service for comprehensive filtering:

### Filter Types

1. **Blacklist Filtering**
   ```json
   "filter_config": {
     "blacklist_tables": [
       "ACRM_DW.RB_BLACK_LIST@ACRM",
       "dssb_de.dim_clients_black_list",
       "BL_No_worker"
     ]
   }
   ```

2. **Device Filtering**
   ```json
   "filter_config": {
     "devices": ["android", "iOS"]
   }
   ```

3. **Push Preference Filtering**
   ```json
   "filter_config": {
     "push_streams": ["Переводы", "Платежи", "Маркет"]
   }
   ```

4. **MAU Filtering**
   ```json
   "filter_config": {
     "mau_only": true
   }
   ```

5. **Product Filtering**
   ```json
   "filter_config": {
     "products": ["Кредитная карта", "Депозит"]
   }
   ```

### Filter Statistics

The service provides detailed statistics about filtering results:
```json
"filter_stats": {
  "initial_count": 100000,
  "after_blacklist": 85000,
  "blacklist_removed": 15000,
  "after_device": 70000,
  "after_mau": 65000,
  "final_count": 60000,
  "total_removed": 40000
}
```

## Campaign Workflow

### Complete Campaign Creation Process

1. **Data Loading** (existing functionality)
   - Query builder for database selection
   - Excel/CSV file upload
   - Product-based selection

2. **Data Filtering** (parquet service integration)
   - Apply blacklist filters
   - Device/platform filtering
   - Push preference filtering
   - MAU coverage filtering
   - Product targeting

3. **Campaign Creation** (new functionality)
   - Generate campaign codes
   - Validate metadata
   - Deploy to multiple tables
   - Send notifications

### Code Generation Logic

#### RB1 CAMPAIGNCODE Generation
```sql
-- Query for next RB1 code
SELECT MAX(CAMPAIGNCODE) 
FROM dssb_ocds.mb01_camp_dict 
WHERE LENGTH(CAMPAIGNCODE) = 10 AND CAMPAIGNCODE LIKE 'C0000%'

-- Increment: C000012345 → C000012346
```

#### RB3 XLS_OW_ID Generation
```sql
-- Query for next RB3 XLS code
SELECT MAX(XLS_OW_ID) 
FROM dssb_ocds.rb3_tr_campaign_dict 
WHERE LENGTH(XLS_OW_ID) = 8 AND XLS_OW_ID LIKE 'KKB_%'

-- Increment: KKB_0123 → KKB_0124
```

## Deployment Options

### Table Deployment Control

```json
"deploy_options": {
  "deploy_metadata": true,    // Campaign metadata tables
  "deploy_targeting": true,   // Targeting tables
  "deploy_users": true,       // User list tables
  "deploy_offlimit": true     // Off-limit tracking
}
```

### Deployment Process

1. **Metadata Deployment**
   - RB1: `dssb_ocds.mb01_camp_dict`
   - RB3: `dssb_ocds.rb3_tr_campaign_dict`

2. **Targeting Deployment**
   - `dssb_ocds.mb22_local_target` - Target users
   - `dssb_ocds.mb21_local_control` - Control groups (if stratification)

3. **User List Deployment**
   - `spss.fd_rb2_campaigns_users` - Main user list
   - `spss.off_limit_campaigns_users` - Off-limit tracking

## Error Handling

### Graceful Error Management

The service provides comprehensive error handling:

1. **Code Generation Errors**
   - Database connection failures → Timestamp-based fallback codes
   - Query errors → Fallback generation logic

2. **Filtering Errors**
   - Parquet file issues → Continue with available data
   - Missing datasets → Log warnings, continue processing

3. **Deployment Errors**
   - Table-specific errors → Continue with other tables
   - Batch operation failures → Detailed error reporting

### Error Response Example

```json
{
  "success": false,
  "campaign_code": "C000012348",
  "deployment_result": {
    "tables_updated": [
      "dssb_ocds.mb01_camp_dict",
      "dssb_ocds.mb22_local_target"
    ],
    "errors": [
      "fd_rb2_campaigns_users: ORA-00001: unique constraint violated",
      "off_limit_campaigns_users: Connection timeout"
    ],
    "success": false
  }
}
```

## Performance Considerations

### Optimization Features

1. **Batch Operations**
   - Bulk insert operations for user data
   - Optimized Oracle queries
   - Connection pooling

2. **Parquet Integration**
   - Cached filtering results
   - Efficient set operations for IIN filtering
   - Lazy loading of datasets

3. **Code Generation**
   - Single query for MAX value lookup
   - Fallback mechanisms for high availability

### Best Practices

1. **Data Filtering**
   - Apply most selective filters first
   - Use parquet caching for repeated operations
   - Monitor filter statistics for optimization

2. **Campaign Deployment**
   - Use transaction rollback for critical errors
   - Deploy in logical order (metadata → targeting → users)
   - Monitor deployment results

3. **Error Recovery**
   - Implement retry logic for transient errors
   - Provide detailed error messages for debugging
   - Log all operations for audit trail

## Integration Examples

### Frontend Integration

```javascript
// Create RB1 campaign
const campaignRequest = {
  campaign_type: 'RB1',
  metadata: {
    campaign_name: 'New Year Promotion',
    stream: 'market',
    channel: 'Push',
    // ... other metadata
  },
  user_iins: selectedUserIINs,
  filter_config: {
    blacklist_tables: selectedBlacklists,
    mau_only: true
  }
};

const response = await fetch('/campaigns/create', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(campaignRequest)
});

const result = await response.json();
```

### Backend Integration

```python
from campaign_service import campaign_service
import pandas as pd

# Prepare user data
user_data = pd.DataFrame({'IIN': ['123456789012', '987654321098']})

# Configure filtering
filter_config = {
    'blacklist_tables': ['ACRM_DW.RB_BLACK_LIST@ACRM'],
    'mau_only': True
}

# Create RB1 campaign
result = await campaign_service.create_rb1_campaign(
    campaign_metadata=metadata_dict,
    user_data=user_data,
    filter_config=filter_config
)
```

## Migration from Market.py

### Functionality Mapping

| Market.py Function | Campaign Service Method |
|---------------------|------------------------|
| `increment_string()` | `generate_next_rb1_code()` |
| `increment_string_XLS()` | `generate_next_rb3_xls_code()` |
| `filter_dataframe()` | `apply_filters_to_data()` |
| Database deployment | `deploy_rb1_campaign()` / `deploy_rb3_campaign()` |

### Migration Benefits

1. **API-Based**: RESTful endpoints instead of Streamlit UI
2. **Scalable**: Multi-user support with authentication
3. **Robust**: Comprehensive error handling and validation
4. **Integrated**: Seamless parquet service integration
5. **Maintainable**: Clean separation of concerns

## Troubleshooting

### Common Issues

1. **Code Generation Failures**
   - Check database connectivity
   - Verify table permissions
   - Review fallback code generation

2. **Filtering Issues**
   - Verify parquet service availability
   - Check dataset configurations
   - Monitor filter statistics

3. **Deployment Errors**
   - Check Oracle table permissions
   - Verify connection strings
   - Review batch size limitations

### Debugging

1. **Enable Debug Logging**
   ```python
   import logging
   logging.getLogger('campaign_service').setLevel(logging.DEBUG)
   ```

2. **Check API Responses**
   - Review deployment_result errors
   - Monitor filter statistics
   - Verify campaign codes

3. **Database Validation**
   - Verify data in campaign tables
   - Check user count consistency
   - Validate metadata accuracy

## Future Enhancements

### Planned Features

1. **Campaign Scheduling**
   - Delayed campaign execution
   - Recurring campaign support
   - Campaign dependency management

2. **Advanced Analytics**
   - Campaign performance metrics
   - Real-time execution monitoring
   - ROI analysis integration

3. **UI Integration**
   - React components for campaign creation
   - Visual filter configuration
   - Campaign dashboard

4. **Workflow Automation**
   - Approval workflows
   - Automated testing
   - Rollback capabilities

This service provides the complete foundation for campaign management, replacing all market.py functionality with a modern, scalable, and maintainable architecture. 