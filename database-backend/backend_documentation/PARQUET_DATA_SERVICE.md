# Parquet Data Service Documentation

## Overview

The Parquet Data Service is a core component of DataQuery Pro that provides efficient access to pre-processed datasets stored as parquet files. This service was designed to replicate and enhance the filtering and targeting functionality from the original market.py system while providing better performance, caching, and API access.

## Purpose

The service manages access to various datasets including:
- **Stop/Black Lists**: Customer exclusion lists from various sources
- **Device Data**: Mobile/web platform usage information
- **Push Preferences**: User communication preferences 
- **Product Data**: Customer product ownership and targeting information
- **Analytics Data**: MAU (Monthly Active Users) and other metrics

## Architecture

### File Structure
```
database-backend/
├── parquet_service.py          # Main service implementation
├── models.py                   # Pydantic models for API
└── Databases/                  # Parquet files directory (production)
    ├── ACRM_DW.RB_BLACK_LIST@ACRM.parquet
    ├── final.parquet
    ├── MAU.parquet
    └── ... (other parquet files)
```

### Key Components

1. **ParquetDataService Class**: Core service managing file access and caching
2. **API Endpoints**: RESTful endpoints for accessing data
3. **Caching System**: In-memory caching with TTL (Time To Live)
4. **Mock Data Support**: Graceful handling when files don't exist (testing)

## Datasets

### Stop/Black Lists (Category: blacklist)
- `ACRM_DW.RB_BLACK_LIST@ACRM` - ACRM Black List
- `dssb_de.dim_clients_black_list` - Clients Black List
- `SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK` - Halyk Job Users
- `SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK` - Bloggers List
- `dssb_app.not_recommend_credits` - Not Recommend Credits
- `DSSB_OCDS.mb11_global_control` - Global Control List
- `BL_No_worker` - No Worker Black List

### ABC Models (Category: abc_model)
- `dssb_app.abc_nbo_only` - ABC NBO Only
- `dssb_app.abc_ptb_models` - ABC PTB Models
- `dssb_app.abc_nbo_and_market` - ABC NBO and Market

### Push/Device Data (Category: push/device)
- `DSSB_SE.UCS_HB_PUSH` - Push Notifications Data
- `DSSB_DE.UCS_PUSH_OFF` - Push Off Events
- `dssb_dm.hb_sessions_fl` - HB Sessions (Device Data)

### Analytics (Category: analytics)
- `dssb_dev.dssb_push_analytics` - Push Analytics (MAU)
- `MAU` - Monthly Active Users

### Products (Category: products)
- `final` - Product Data
- `dssb_app.products_per_fl_prod` - Products per FL Production

## API Endpoints

### 1. Get Available Datasets
```http
GET /parquet/datasets
```

**Response:**
```json
{
  "datasets": {
    "ACRM_DW.RB_BLACK_LIST@ACRM": {
      "file": "ACRM_DW.RB_BLACK_LIST@ACRM.parquet",
      "description": "ACRM Black List",
      "category": "blacklist",
      "columns": ["IIN"],
      "available": true,
      "file_path": "Databases/ACRM_DW.RB_BLACK_LIST@ACRM.parquet",
      "cached": false
    }
  },
  "total_count": 15,
  "available_count": 12,
  "cached_count": 3
}
```

### 2. Get Dataset Information
```http
GET /parquet/datasets/{dataset_name}
```

**Example:**
```http
GET /parquet/datasets/final
```

**Response:**
```json
{
  "file": "final.parquet",
  "description": "Product Data",
  "category": "products",
  "columns": ["IIN", "sku_level1"],
  "available": true,
  "file_path": "Databases/final.parquet",
  "cached": true,
  "row_count": 150000,
  "column_count": 2,
  "actual_columns": ["IIN", "sku_level1"]
}
```

### 3. Filter IINs by Data Type
```http
POST /parquet/filter
```

**Request Body:**
```json
{
  "filter_type": "blacklist",
  "parameters": {
    "tables": ["ACRM_DW.RB_BLACK_LIST@ACRM", "dssb_de.dim_clients_black_list"]
  }
}
```

**Response:**
```json
{
  "success": true,
  "filter_type": "blacklist",
  "iins": ["123456789012", "987654321098"],
  "count": 2,
  "message": "Filtered 2 IINs from 2 blacklist tables",
  "parameters_used": {
    "tables": ["ACRM_DW.RB_BLACK_LIST@ACRM", "dssb_de.dim_clients_black_list"]
  }
}
```

### 4. Get Datasets by Category
```http
GET /parquet/datasets/category/{category}
```

**Example:**
```http
GET /parquet/datasets/category/blacklist
```

### 5. Cache Management
```http
GET /parquet/cache/stats
POST /parquet/cache/clear?dataset_name={optional}
```

### 6. Load Dataset
```http
POST /parquet/datasets/{dataset_name}/load?use_cache=true
```

## Filter Types

### 1. Blacklist Filter
Excludes IINs found in specified blacklist tables.

```json
{
  "filter_type": "blacklist",
  "parameters": {
    "tables": ["ACRM_DW.RB_BLACK_LIST@ACRM", "BL_No_worker"]
  }
}
```

### 2. Device Filter
Finds IINs based on device/platform usage.

```json
{
  "filter_type": "device", 
  "parameters": {
    "devices": ["android", "iOS"]
  }
}
```

### 3. Push Filter
Finds IINs based on push notification preferences.

```json
{
  "filter_type": "push",
  "parameters": {
    "streams": ["Переводы", "Платежи", "Маркет"]
  }
}
```

### 4. MAU Filter
Finds IINs covered by Monthly Active Users data.

```json
{
  "filter_type": "mau",
  "parameters": {}
}
```

### 5. Products Filter
Finds IINs based on product ownership.

```json
{
  "filter_type": "products",
  "parameters": {
    "products": ["Кредитная карта", "Депозит"]
  }
}
```

## Caching System

### Features
- **Automatic Caching**: Datasets are cached automatically when loaded
- **TTL Support**: Cache expires after 1 hour by default
- **Memory Efficient**: Only loads datasets when requested
- **Cache Statistics**: Monitor cache usage and performance

### Cache Management
```python
# Clear specific dataset cache
parquet_service.clear_cache("final")

# Clear all cache
parquet_service.clear_cache()

# Check cache status
stats = parquet_service.get_cache_stats()
```

## Environment Support

### Production Environment
- Parquet files exist in `Databases/` directory
- Full functionality available
- Real data filtering and targeting

### Testing Environment
- Mock datasets created when files don't exist
- Empty DataFrames with correct column structure
- API functionality preserved for testing

## Integration with Market.py Functionality

The service provides direct replacements for market.py functions:

| Market.py Function | Parquet Service Method |
|-------------------|----------------------|
| `fetch_data(table)` | `load_dataset(dataset_name)` |
| `fetch_data_device(devices)` | `get_device_filtered_iins(devices)` |
| `fetch_data_extra_filters(streams)` | `get_push_filtered_iins(streams)` |
| `filter_df_by_iin(df)` | `get_mau_iins()` |
| `load_products_from_parquet()` | `get_product_iins(products)` |

## Performance Considerations

### Optimization Features
1. **Lazy Loading**: Files only loaded when requested
2. **Caching**: Frequently used datasets stay in memory
3. **Efficient Filtering**: Set operations for IIN filtering
4. **Batch Processing**: Handle large datasets efficiently

### Best Practices
1. Use caching for frequently accessed datasets
2. Clear cache periodically to free memory
3. Monitor cache statistics for optimization
4. Use specific filters rather than loading entire datasets

## Error Handling

### File Not Found
- Gracefully handles missing parquet files
- Returns empty mock datasets for testing
- Logs warnings but doesn't break functionality

### Data Validation
- Checks for expected columns in datasets
- Reports missing columns as warnings
- Continues operation with available data

### API Error Responses
- Detailed error messages for debugging
- HTTP status codes indicating error types
- Consistent error response format

## Usage Examples

### Frontend Integration
```javascript
// Get available datasets
const datasets = await fetch('/parquet/datasets');

// Filter IINs by blacklists
const blacklistFilter = {
  filter_type: 'blacklist',
  parameters: {
    tables: ['ACRM_DW.RB_BLACK_LIST@ACRM']
  }
};
const filteredIINs = await fetch('/parquet/filter', {
  method: 'POST',
  body: JSON.stringify(blacklistFilter)
});

// Check cache status
const cacheStats = await fetch('/parquet/cache/stats');
```

### Backend Integration
```python
from parquet_service import parquet_service

# Load dataset
df = parquet_service.load_dataset('final')

# Get blacklist IINs
blacklisted = parquet_service.get_blacklist_iins([
    'ACRM_DW.RB_BLACK_LIST@ACRM',
    'BL_No_worker'
])

# Filter by products
product_iins = parquet_service.get_product_iins(['Кредитная карта'])
```

## Migration from Market.py

### Step-by-Step Migration
1. **Identify Data Sources**: Map parquet file usage in market.py
2. **Replace File Access**: Use parquet service methods instead of direct file access
3. **Update Filtering Logic**: Use service filter methods
4. **Add API Integration**: Connect frontend to parquet endpoints
5. **Test Functionality**: Verify filtering behavior matches market.py

### Common Migration Patterns
```python
# Before (market.py)
df = pd.read_parquet('Databases/final.parquet')
filtered = df[df['sku_level1'].isin(selected_products)]

# After (parquet service)
product_iins = parquet_service.get_product_iins(selected_products)
```

## Future Enhancements

### Planned Features
- **Data Refresh**: Automatic detection of file updates
- **Compression**: Better memory usage for large datasets
- **Distributed Caching**: Support for multiple backend instances
- **Data Validation**: Schema validation for parquet files
- **Metrics**: Performance monitoring and alerts

### Extensibility
- Easy to add new dataset types
- Configurable caching strategies
- Plugin system for custom filters
- API versioning support

## Troubleshooting

### Common Issues
1. **Files Not Found**: Check `Databases/` directory exists and has correct permissions
2. **Memory Issues**: Monitor cache usage and clear when needed
3. **Performance**: Check file sizes and consider data optimization
4. **Column Mismatch**: Verify parquet files have expected column structure

### Debugging
- Enable debug logging for detailed operation info
- Use cache statistics to monitor performance
- Check dataset info endpoints for file status
- Verify environment setup for production vs testing

This service provides a robust foundation for all data filtering and targeting operations previously handled by market.py, with enhanced performance, caching, and API integration capabilities. 