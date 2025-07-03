from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime, date

# Request Models
class QueryRequest(BaseModel):
    database_id: str
    table: str
    columns: Optional[List[str]] = None
    filters: Optional[List[Dict[str, Any]]] = None
    sort_by: Optional[str] = None
    sort_order: Optional[str] = "ASC"
    limit: Optional[int] = 100

class ConnectionTestRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

class SaveQueryRequest(BaseModel):
    name: str
    description: Optional[str] = None
    sql: str
    database_id: str
    table: str

class DataRequest(BaseModel):
    table: str
    page: Optional[int] = 1
    limit: Optional[int] = 25
    search: Optional[str] = None
    sort_by: Optional[str] = None
    sort_order: Optional[str] = "asc"
    filters: Optional[Dict[str, Any]] = None

# Authentication Models
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user: Dict[str, Any]
    expires_in: int

class UserResponse(BaseModel):
    username: str
    name: str
    role: str
    permissions: List[str]

# Response Models
class DatabaseResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None

class TableResponse(BaseModel):
    name: str
    description: Optional[str] = None
    columns_count: Optional[int] = None

class ColumnResponse(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    nullable: Optional[bool] = True
    options: Optional[List[str]] = None

class QueryResultResponse(BaseModel):
    success: bool
    columns: Optional[List[str]] = None
    data: Optional[List[Dict[str, Any]]] = None
    row_count: Optional[int] = None
    message: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[str] = None

class QueryHistoryResponse(BaseModel):
    id: int
    sql: str
    database_id: str
    table: str
    execution_time: str
    status: str
    created_at: datetime
    row_count: int
    user: Optional[str] = None

class SavedQueryResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    sql: str
    database_id: str
    table: str
    created_at: datetime
    updated_at: Optional[datetime] = None

class ConnectionTestResponse(BaseModel):
    status: str
    message: str
    connected: bool
    response_time: Optional[str] = None

class DatabaseConnectionStatus(BaseModel):
    """Individual database connection status"""
    status: str
    message: str
    connected: bool

class AllConnectionsTestResponse(BaseModel):
    """Enhanced response for testing all database connections"""
    dssb_app: DatabaseConnectionStatus
    spss: DatabaseConnectionStatus
    dssb_ocds: DatabaseConnectionStatus
    ed_ocds: DatabaseConnectionStatus
    overall_status: str
    message: str
    successful_connections: int
    total_connections: int

class DataResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_count: int
    page: int
    limit: int
    total_pages: int

class StatsResponse(BaseModel):
    total_queries: int
    active_databases: int
    total_users: int
    avg_response_time: str

# Settings Models
class DatabaseSettings(BaseModel):
    host: str
    port: str
    database: str
    username: str
    ssl: bool = False
    connection_timeout: int = 30

class APISettings(BaseModel):
    base_url: str
    timeout: int = 30000
    retries: int = 3
    api_key: Optional[str] = None

class UserPreferences(BaseModel):
    default_rows_per_page: int = 25
    date_format: str = "dd.MM.yyyy"
    timezone: str = "Europe/Moscow"
    theme: str = "light"
    auto_refresh: bool = False
    refresh_interval: int = 30

class SettingsResponse(BaseModel):
    database: DatabaseSettings
    api: APISettings
    preferences: UserPreferences

class ErrorResponse(BaseModel):
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None

# Theory Management Models
class CreateTheoryRequest(BaseModel):
    theory_name: str
    theory_description: str
    theory_start_date: str  # YYYY-MM-DD format
    theory_end_date: str    # YYYY-MM-DD format
    user_iins: List[str]    # List of IIN values from query results

class TheoryResponse(BaseModel):
    theory_id: str  # Changed from int to str to support decimal IDs like "1.1"
    theory_name: str
    theory_description: str
    load_date: str
    theory_start_date: str
    theory_end_date: str
    user_count: int
    is_active: bool
    created_by: str

class TheoryCreateResponse(BaseModel):
    success: bool
    message: str
    theory_id: Optional[str] = None  # Changed from int to str to support decimal IDs
    users_added: Optional[int] = None

# Parquet Data Service Models
class ParquetDatasetInfo(BaseModel):
    """Information about a parquet dataset"""
    file: str
    description: str
    category: str
    columns: List[str]
    available: bool
    file_path: str
    cached: bool
    cache_timestamp: Optional[str] = None
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    actual_columns: Optional[List[str]] = None

class ParquetDatasetsResponse(BaseModel):
    """Response for available datasets"""
    datasets: Dict[str, ParquetDatasetInfo]
    total_count: int
    available_count: int
    cached_count: int

class ParquetCacheStatsResponse(BaseModel):
    """Response for cache statistics"""
    cached_datasets: List[str]
    cache_size: int
    cache_ttl_hours: float
    timestamps: Dict[str, str]

class ParquetFilterRequest(BaseModel):
    """Request for filtering IINs using parquet data"""
    filter_type: str  # 'blacklist', 'device', 'push', 'mau', 'products'
    parameters: Dict[str, Any] = {}  # Type-specific parameters

class ParquetFilterResponse(BaseModel):
    """Response for IIN filtering"""
    success: bool
    filter_type: str
    iins: List[str]
    count: int
    message: str
    parameters_used: Dict[str, Any] = {}

# Campaign Management Models
class CampaignFilterConfig(BaseModel):
    """Configuration for filtering campaign data"""
    blacklist_tables: Optional[List[str]] = None
    devices: Optional[List[str]] = None
    push_streams: Optional[List[str]] = None
    mau_only: Optional[bool] = False
    products: Optional[List[str]] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    gender: Optional[str] = None  # 'M' or 'F'
    filials: Optional[List[str]] = None
    local_control_streams: Optional[List[str]] = None
    local_target_streams: Optional[List[str]] = None
    rb3_control_streams: Optional[List[str]] = None
    rb3_target_streams: Optional[List[str]] = None
    previous_campaigns: Optional[List[str]] = None
    cleanup_date: Optional[str] = None
    phone_required: Optional[bool] = False
    # New sum_columns functionality
    info_columns: Optional[List[str]] = ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU']
    sum_columns: Optional[List[str]] = None
    min_sum: Optional[float] = None

class CampaignDeployOptions(BaseModel):
    """Options for campaign deployment"""
    deploy_metadata: bool = True
    deploy_targeting: bool = True
    deploy_users: bool = True
    deploy_offlimit: bool = True

class RB1CampaignMetadata(BaseModel):
    """RB1 Campaign metadata"""
    campaign_name: str
    campaign_desc: str
    stream: str
    sub_stream: str
    target_action: str
    channel: str  # 'Push', 'POP-UP', 'СМС'
    campaign_type: str
    campaign_text: str
    campaign_text_kz: Optional[str] = None
    campaign_model: Optional[str] = None
    cds_launcher: Optional[str] = None
    short_desc: str
    date_start: date
    date_end: date
    out_date: date
    camp_cnt: Optional[str] = None

class RB3CampaignMetadata(RB1CampaignMetadata):
    """RB3 Campaign metadata (extends RB1)"""
    bonus: Optional[str] = None
    characteristic_json: Optional[str] = None

class CampaignCreateRequest(BaseModel):
    """Request to create a campaign"""
    campaign_type: str  # 'RB1' or 'RB3'
    metadata: Dict[str, Any]  # Will be validated as RB1 or RB3 metadata
    user_iins: List[str]  # List of user IINs for the campaign
    filter_config: Optional[CampaignFilterConfig] = None
    deploy_options: Optional[CampaignDeployOptions] = None

class CampaignFilterStats(BaseModel):
    """Statistics from campaign data filtering"""
    initial_count: int
    final_count: int
    blacklist_removed: Optional[int] = None
    after_blacklist: Optional[int] = None
    after_device: Optional[int] = None
    after_push: Optional[int] = None
    after_mau: Optional[int] = None
    after_products: Optional[int] = None
    total_removed: Optional[int] = None

class CampaignDeploymentResult(BaseModel):
    """Result of campaign deployment"""
    campaign_code: str
    tables_updated: List[str]
    total_users: int
    errors: List[str]
    success: bool

class CampaignCreateResponse(BaseModel):
    """Response from campaign creation"""
    success: bool
    campaign_code: str
    campaign_type: str
    xls_ow_id: Optional[str] = None  # For RB3 campaigns
    filter_stats: CampaignFilterStats
    deployment_result: CampaignDeploymentResult
    message: str

class CampaignCodeResponse(BaseModel):
    """Response for campaign code generation"""
    campaign_code: str
    campaign_type: str
    xls_ow_id: Optional[str] = None
    generated_at: datetime

class CampaignListItem(BaseModel):
    """Campaign item for listing"""
    campaign_code: str
    campaign_type: str
    campaign_name: str
    stream: str
    channel: str
    date_start: date
    date_end: date
    user_count: Optional[int] = None
    created_at: datetime
    status: str

class CampaignListResponse(BaseModel):
    """Response for campaign listing"""
    campaigns: List[CampaignListItem]
    total_count: int
    rb1_count: int
    rb3_count: int 