"""
Campaign Management Service for DataQuery Pro

Handles RB1/RB3 campaign creation, CAMPAIGNCODE generation, and deployment
to multiple Oracle tables. Replaces the campaign functionality from market.py.
"""

import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
import pandas as pd
from database import (
    get_connection_DSSB_APP, get_connection_DSSB_OCDS, 
    get_connection_SPSS, get_connection_ED_OCDS
)
from parquet_service import parquet_service

# Configure logging
logger = logging.getLogger(__name__)

class CampaignCodeService:
    """Service for generating campaign codes"""
    
    @staticmethod
    async def generate_next_rb1_code() -> str:
        """Generate next available RB1 CAMPAIGNCODE (format: C000012345)"""
        try:
            with get_connection_DSSB_OCDS() as conn:
                cursor = conn.cursor()
                query = """
                    SELECT MAX(CAMPAIGNCODE) as CAMPAIGNCODE 
                    FROM dssb_ocds.mb01_camp_dict 
                    WHERE LENGTH(CAMPAIGNCODE) = 10 AND CAMPAIGNCODE LIKE 'C0000%'
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0]:
                    # Extract numeric part and increment
                    current_code = result[0]
                    numeric_part = int(current_code[1:])  # Remove 'C' prefix
                    next_numeric = numeric_part + 1
                    next_code = f"C{next_numeric:09d}"  # Format with leading zeros
                    logger.info(f"Generated next RB1 code: {next_code}")
                    return next_code
                else:
                    # First campaign
                    first_code = "C000000001"
                    logger.info(f"Generated first RB1 code: {first_code}")
                    return first_code
                    
        except Exception as e:
            logger.error(f"Error generating RB1 campaign code: {e}")
            # Fallback to timestamp-based code
            timestamp = datetime.now().strftime("%m%d%H%M%S")
            fallback_code = f"C{timestamp:0>9}"[:10]
            logger.warning(f"Using fallback RB1 code: {fallback_code}")
            return fallback_code
    
    @staticmethod
    async def generate_next_rb3_xls_code() -> str:
        """Generate next available RB3 XLS_OW_ID (format: KKB_0123)"""
        try:
            with get_connection_DSSB_OCDS() as conn:
                cursor = conn.cursor()
                query = """
                    SELECT MAX(XLS_OW_ID) as XLS_OW_ID 
                    FROM dssb_ocds.rb3_tr_campaign_dict 
                    WHERE LENGTH(XLS_OW_ID) = 8 AND XLS_OW_ID LIKE 'KKB_%'
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0]:
                    # Extract numeric part and increment
                    current_code = result[0]  # KKB_0123
                    numeric_part = int(current_code[4:])  # Extract 0123
                    next_numeric = numeric_part + 1
                    next_code = f"KKB_{next_numeric:04d}"  # Format with leading zeros
                    logger.info(f"Generated next RB3 XLS code: {next_code}")
                    return next_code
                else:
                    # First RB3 campaign
                    first_code = "KKB_0001"
                    logger.info(f"Generated first RB3 XLS code: {first_code}")
                    return first_code
                    
        except Exception as e:
            logger.error(f"Error generating RB3 XLS code: {e}")
            # Fallback to timestamp-based code
            timestamp = datetime.now().strftime("%H%M")
            fallback_code = f"KKB_{timestamp:0>4}"
            logger.warning(f"Using fallback RB3 XLS code: {fallback_code}")
            return fallback_code

class CampaignDataProcessor:
    """Service for processing and filtering campaign data"""
    
    @staticmethod
    def load_rb_feature_store_data(
        info_columns: List[str],
        sum_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Load data from rb_feature_store with specified columns
        Replicates the original market.py data loading logic
        """
        try:
            # Load parquet file
            parquet_path = os.path.join(os.getenv('PARQUET_OUTPUT_DIR', 'Databases'), 'dssb_app.rb_feature_store.parquet')
            
            if not os.path.exists(parquet_path):
                logger.error(f"rb_feature_store.parquet not found at {parquet_path}")
                raise FileNotFoundError(f"rb_feature_store.parquet not found")
            
            # Determine columns to load
            all_columns = info_columns.copy()
            if sum_columns:
                all_columns.extend(sum_columns)
            
            # Remove duplicates while preserving order
            unique_columns = []
            for col in all_columns:
                if col not in unique_columns:
                    unique_columns.append(col)
            
            # Load data with specified columns
            df = pd.read_parquet(parquet_path)
            
            # Check if all requested columns exist
            missing_columns = [col for col in unique_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing columns in rb_feature_store: {missing_columns}")
                # Use only available columns
                available_columns = [col for col in unique_columns if col in df.columns]
                df = df[available_columns]
            else:
                df = df[unique_columns]
            
            logger.info(f"Loaded {len(df)} records from rb_feature_store with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error loading rb_feature_store data: {e}")
            raise
    
    @staticmethod
    def apply_sum_columns_logic(
        data: pd.DataFrame,
        sum_columns: List[str],
        min_sum: Optional[float] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Apply sum_columns logic: sum selected columns and filter by minimum sum
        Replicates the original market.py add_column_sum and sum filtering logic
        """
        stats = {}
        
        try:
            # Validate that all sum_columns exist in the data
            missing_columns = [col for col in sum_columns if col not in data.columns]
            if missing_columns:
                logger.warning(f"Missing sum_columns: {missing_columns}")
                # Use only available columns
                available_sum_columns = [col for col in sum_columns if col in data.columns]
                if not available_sum_columns:
                    logger.error("No valid sum_columns found in data")
                    return data, {"error": "No valid sum_columns found"}
                sum_columns = available_sum_columns
            
            stats["sum_columns_used"] = sum_columns
            stats["initial_count"] = len(data)
            
            # Create Column_sum by summing the selected columns
            # Handle missing values by filling with 0
            data = data.copy()
            data[sum_columns] = data[sum_columns].fillna(0)
            data['Column_sum'] = data[sum_columns].sum(axis=1)
            
            logger.info(f"Created Column_sum from {len(sum_columns)} columns")
            stats["column_sum_created"] = True
            stats["sum_range"] = {
                "min": float(data['Column_sum'].min()),
                "max": float(data['Column_sum'].max()),
                "mean": float(data['Column_sum'].mean())
            }
            
            # Apply minimum sum filter if specified
            if min_sum is not None and min_sum > 0:
                initial_count = len(data)
                data = data[data['Column_sum'] >= min_sum]
                final_count = len(data)
                
                stats["min_sum_applied"] = min_sum
                stats["removed_by_sum_filter"] = initial_count - final_count
                stats["final_count"] = final_count
                
                logger.info(f"Sum filter (>= {min_sum}) kept {final_count} of {initial_count} records")
            else:
                stats["final_count"] = len(data)
            
            return data, stats
            
        except Exception as e:
            logger.error(f"Error applying sum_columns logic: {e}")
            return data, {"error": str(e)}
    
    @staticmethod
    def apply_filters_to_data(
        base_data: pd.DataFrame, 
        filter_config: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        Apply various filters to base data using parquet service
        Returns: (filtered_dataframe, filter_stats)
        """
        stats = {"initial_count": len(base_data)}
        current_data = base_data.copy()
        
        try:
            # Apply blacklist filters
            if filter_config.get("blacklist_tables"):
                blacklist_iins = parquet_service.get_blacklist_iins(
                    filter_config["blacklist_tables"]
                )
                if blacklist_iins:
                    current_data = current_data[~current_data['IIN'].isin(blacklist_iins)]
                    stats["after_blacklist"] = len(current_data)
                    stats["blacklist_removed"] = stats["initial_count"] - stats["after_blacklist"]
                    logger.info(f"Blacklist filter removed {stats['blacklist_removed']} records")
            
            # Apply device filters
            if filter_config.get("devices"):
                device_iins = parquet_service.get_device_filtered_iins(
                    filter_config["devices"]
                )
                if device_iins:
                    current_data = current_data[current_data['IIN'].isin(device_iins)]
                    stats["after_device"] = len(current_data)
                    logger.info(f"Device filter kept {stats['after_device']} records")
            
            # Apply push preference filters
            if filter_config.get("push_streams"):
                push_excluded_iins = parquet_service.get_push_filtered_iins(
                    filter_config["push_streams"]
                )
                if push_excluded_iins:
                    current_data = current_data[~current_data['IIN'].isin(push_excluded_iins)]
                    stats["after_push"] = len(current_data)
                    logger.info(f"Push filter kept {stats['after_push']} records")
            
            # Apply MAU filter
            if filter_config.get("mau_only", False):
                mau_iins = parquet_service.get_mau_iins()
                if mau_iins:
                    current_data = current_data[current_data['IIN'].isin(mau_iins)]
                    stats["after_mau"] = len(current_data)
                    logger.info(f"MAU filter kept {stats['after_mau']} records")
            
            # Apply product filters
            if filter_config.get("products"):
                product_iins = parquet_service.get_product_iins(
                    filter_config["products"]
                )
                if product_iins:
                    current_data = current_data[current_data['IIN'].isin(product_iins)]
                    stats["after_products"] = len(current_data)
                    logger.info(f"Product filter kept {stats['after_products']} records")
            
            stats["final_count"] = len(current_data)
            stats["total_removed"] = stats["initial_count"] - stats["final_count"]
            
            return current_data, stats
            
        except Exception as e:
            logger.error(f"Error applying filters: {e}")
            return current_data, stats

class CampaignDeploymentService:
    """Service for deploying campaigns to Oracle tables"""
    
    @staticmethod
    def deploy_rb1_campaign(
        campaign_code: str,
        campaign_metadata: Dict[str, Any],
        user_data: pd.DataFrame,
        deploy_options: Dict[str, bool]
    ) -> Dict[str, Any]:
        """Deploy RB1 campaign to multiple Oracle tables"""
        results = {
            "campaign_code": campaign_code,
            "tables_updated": [],
            "total_users": len(user_data),
            "errors": []
        }
        
        try:
            # 1. Deploy to mb01_camp_dict (campaign metadata)
            if deploy_options.get("deploy_metadata", True):
                try:
                    CampaignDeploymentService._deploy_to_mb01_camp_dict(
                        campaign_code, campaign_metadata
                    )
                    results["tables_updated"].append("dssb_ocds.mb01_camp_dict")
                except Exception as e:
                    results["errors"].append(f"mb01_camp_dict: {str(e)}")
            
            # 2. Deploy to mb22_local_target (targeting)
            if deploy_options.get("deploy_targeting", True):
                try:
                    CampaignDeploymentService._deploy_to_mb22_local_target(
                        campaign_code, campaign_metadata, user_data
                    )
                    results["tables_updated"].append("dssb_ocds.mb22_local_target")
                except Exception as e:
                    results["errors"].append(f"mb22_local_target: {str(e)}")
            
            # 3. Deploy to fd_rb2_campaigns_users (main user list)
            if deploy_options.get("deploy_users", True):
                try:
                    CampaignDeploymentService._deploy_to_fd_rb2_campaigns_users(
                        campaign_code, campaign_metadata, user_data
                    )
                    results["tables_updated"].append("spss.fd_rb2_campaigns_users")
                except Exception as e:
                    results["errors"].append(f"fd_rb2_campaigns_users: {str(e)}")
            
            # 4. Deploy to off_limit_campaigns_users (tracking)
            if deploy_options.get("deploy_offlimit", True):
                try:
                    CampaignDeploymentService._deploy_to_off_limit_campaigns_users(
                        campaign_code, campaign_metadata, user_data
                    )
                    results["tables_updated"].append("spss.off_limit_campaigns_users")
                except Exception as e:
                    results["errors"].append(f"off_limit_campaigns_users: {str(e)}")
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            logger.error(f"Error deploying RB1 campaign: {e}")
            results["errors"].append(f"General deployment error: {str(e)}")
            results["success"] = False
        
        return results
    
    @staticmethod
    def deploy_rb3_campaign(
        campaign_code: str,
        campaign_metadata: Dict[str, Any],
        user_data: pd.DataFrame,
        deploy_options: Dict[str, bool]
    ) -> Dict[str, Any]:
        """Deploy RB3 campaign to multiple Oracle tables"""
        results = {
            "campaign_code": campaign_code,
            "tables_updated": [],
            "total_users": len(user_data),
            "errors": []
        }
        
        try:
            # 1. Deploy to rb3_tr_campaign_dict (RB3 metadata)
            if deploy_options.get("deploy_metadata", True):
                try:
                    CampaignDeploymentService._deploy_to_rb3_tr_campaign_dict(
                        campaign_code, campaign_metadata
                    )
                    results["tables_updated"].append("dssb_ocds.rb3_tr_campaign_dict")
                except Exception as e:
                    results["errors"].append(f"rb3_tr_campaign_dict: {str(e)}")
            
            # RB3 campaigns also use the same user tables as RB1
            # 2. Deploy to mb22_local_target
            if deploy_options.get("deploy_targeting", True):
                try:
                    CampaignDeploymentService._deploy_to_mb22_local_target(
                        campaign_code, campaign_metadata, user_data
                    )
                    results["tables_updated"].append("dssb_ocds.mb22_local_target")
                except Exception as e:
                    results["errors"].append(f"mb22_local_target: {str(e)}")
            
            # 3. Deploy to fd_rb2_campaigns_users
            if deploy_options.get("deploy_users", True):
                try:
                    CampaignDeploymentService._deploy_to_fd_rb2_campaigns_users(
                        campaign_code, campaign_metadata, user_data
                    )
                    results["tables_updated"].append("spss.fd_rb2_campaigns_users")
                except Exception as e:
                    results["errors"].append(f"fd_rb2_campaigns_users: {str(e)}")
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            logger.error(f"Error deploying RB3 campaign: {e}")
            results["errors"].append(f"General deployment error: {str(e)}")
            results["success"] = False
        
        return results
    
    @staticmethod
    def _deploy_to_mb01_camp_dict(campaign_code: str, metadata: Dict[str, Any]):
        """Deploy RB1 campaign metadata to mb01_camp_dict table"""
        with get_connection_DSSB_OCDS() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO dssb_ocds.mb01_camp_dict (
                    CAMPAIGNCODE, STREAM, SUB_STREAM, TARGET_ACTION, CHANNEL,
                    CAMPAIGN_TYPE, CAMPAIGN_NAME, CAMPAIGN_DESC, CAMPAIGN_TEXT,
                    CAMPAIGN_MODEL, CDS_LAUNCHER, CAMPAIGN_TEXT_KZ, OUT_DATE,
                    CAMP_CNT, INSERT_DATETIME
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15
                )
            """
            
            cursor.execute(insert_query, [
                campaign_code,
                metadata.get('stream'),
                metadata.get('sub_stream'),
                metadata.get('target_action'),
                metadata.get('channel'),
                metadata.get('campaign_type'),
                metadata.get('campaign_name'),
                metadata.get('campaign_desc'),
                metadata.get('campaign_text'),
                metadata.get('campaign_model'),
                metadata.get('cds_launcher'),
                metadata.get('campaign_text_kz'),
                metadata.get('out_date'),
                metadata.get('camp_cnt'),
                datetime.now()
            ])
            
            conn.commit()
            logger.info(f"Deployed RB1 metadata for campaign {campaign_code}")
    
    @staticmethod
    def _deploy_to_rb3_tr_campaign_dict(campaign_code: str, metadata: Dict[str, Any]):
        """Deploy RB3 campaign metadata to rb3_tr_campaign_dict table"""
        with get_connection_DSSB_OCDS() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO dssb_ocds.rb3_tr_campaign_dict (
                    CAMPAIGNCODE, DATE_START, DATE_END, XLS_OW_ID, TARGET_ACTION,
                    BONUS, CHARACTERISTIC_JSON
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7
                )
            """
            
            cursor.execute(insert_query, [
                campaign_code,
                metadata.get('date_start'),
                metadata.get('date_end'),
                metadata.get('xls_ow_id'),
                metadata.get('target_action'),
                metadata.get('bonus'),
                metadata.get('characteristic_json')
            ])
            
            conn.commit()
            logger.info(f"Deployed RB3 metadata for campaign {campaign_code}")
    
    @staticmethod
    def _deploy_to_mb22_local_target(
        campaign_code: str, 
        metadata: Dict[str, Any], 
        user_data: pd.DataFrame
    ):
        """Deploy campaign users to mb22_local_target table"""
        with get_connection_DSSB_OCDS() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO dssb_ocds.mb22_local_target (
                    CAMPAIGNCODE, IIN, P_SID, STREAM, DATE_START, DATE_END, INSET_DATETIME
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7
                )
            """
            
            # Prepare batch data
            batch_data = []
            for _, row in user_data.iterrows():
                batch_data.append([
                    campaign_code,
                    row['IIN'],
                    row.get('P_SID', row['IIN']),  # Use IIN as P_SID if not available
                    metadata.get('stream'),
                    metadata.get('date_start'),
                    metadata.get('date_end'),
                    datetime.now()
                ])
            
            # Execute batch insert
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            
            logger.info(f"Deployed {len(batch_data)} users to mb22_local_target for campaign {campaign_code}")
    
    @staticmethod
    def _deploy_to_fd_rb2_campaigns_users(
        campaign_code: str, 
        metadata: Dict[str, Any], 
        user_data: pd.DataFrame
    ):
        """Deploy campaign users to fd_rb2_campaigns_users table"""
        with get_connection_SPSS() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO fd_rb2_campaigns_users (
                    CAMPAIGNCODE, IIN, P_SID, UPLOAD_DATE, SHORT_DESC
                ) VALUES (
                    :1, :2, :3, :4, :5
                )
            """
            
            # Prepare batch data
            batch_data = []
            for _, row in user_data.iterrows():
                batch_data.append([
                    campaign_code,
                    str(row['IIN']).zfill(12),  # Ensure 12-digit string format
                    int(row.get('P_SID', row['IIN'])),  # Ensure integer format
                    metadata.get('date_start'),
                    metadata.get('short_desc')
                ])
            
            # Execute batch insert
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            
            logger.info(f"Deployed {len(batch_data)} users to fd_rb2_campaigns_users for campaign {campaign_code}")
    
    @staticmethod
    def _deploy_to_off_limit_campaigns_users(
        campaign_code: str, 
        metadata: Dict[str, Any], 
        user_data: pd.DataFrame
    ):
        """Deploy campaign users to off_limit_campaigns_users table"""
        with get_connection_SPSS() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO off_limit_campaigns_users (
                    CAMPAIGNCODE, IIN, P_SID, UPLOAD_DATE, SHORT_DESC
                ) VALUES (
                    :1, :2, :3, :4, :5
                )
            """
            
            # Prepare batch data
            batch_data = []
            for _, row in user_data.iterrows():
                batch_data.append([
                    campaign_code,
                    row['IIN'],
                    row.get('P_SID', row['IIN']),
                    metadata.get('date_start'),
                    metadata.get('short_desc')
                ])
            
            # Execute batch insert
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            
            logger.info(f"Deployed {len(batch_data)} users to off_limit_campaigns_users for campaign {campaign_code}")

class CampaignService:
    """Main campaign management service"""
    
    def __init__(self):
        self.code_service = CampaignCodeService()
        self.data_processor = CampaignDataProcessor()
        self.deployment_service = CampaignDeploymentService()
    
    def load_rb_automatic_launch_data(
        self,
        filter_config: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load and process data for РБ Автоматический запуск workflow
        Replicates the original market.py automatic launch logic
        """
        try:
            # Extract configuration
            info_columns = filter_config.get('info_columns', ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'])
            sum_columns = filter_config.get('sum_columns', [])
            min_sum = filter_config.get('min_sum')
            
            stats = {
                "workflow": "rb_automatic_launch",
                "info_columns_used": info_columns,
                "sum_columns_used": sum_columns
            }
            
            # 1. Load base data from rb_feature_store
            logger.info(f"Loading RB automatic launch data with {len(info_columns)} info columns and {len(sum_columns)} sum columns")
            
            base_data = self.data_processor.load_rb_feature_store_data(
                info_columns=info_columns,
                sum_columns=sum_columns if sum_columns else None
            )
            
            stats["initial_count"] = len(base_data)
            current_data = base_data
            
            # 2. Apply sum_columns logic if specified
            if sum_columns:
                logger.info(f"Applying sum_columns logic with {len(sum_columns)} columns")
                current_data, sum_stats = self.data_processor.apply_sum_columns_logic(
                    data=current_data,
                    sum_columns=sum_columns,
                    min_sum=min_sum
                )
                stats.update(sum_stats)
                logger.info(f"Sum columns processing: {stats.get('final_count', len(current_data))} records remain")
            
            # 3. Apply other filters (blacklist, device, push, etc.)
            if any(filter_config.get(key) for key in ['blacklist_tables', 'devices', 'push_streams', 'mau_only', 'products']):
                logger.info("Applying additional filters (blacklist, device, push, MAU, products)")
                current_data, filter_stats = self.data_processor.apply_filters_to_data(
                    base_data=current_data,
                    filter_config=filter_config
                )
                
                # Merge filter stats
                for key, value in filter_stats.items():
                    if key not in stats:
                        stats[key] = value
                
                logger.info(f"Additional filters applied: {len(current_data)} records remain")
            
            stats["final_count"] = len(current_data)
            stats["total_processed"] = stats["initial_count"] - stats["final_count"]
            
            logger.info(f"RB automatic launch complete: {stats['final_count']} records ready for campaign")
            
            return current_data, stats
            
        except Exception as e:
            logger.error(f"Error in RB automatic launch workflow: {e}")
            raise
    
    async def create_rb1_campaign(
        self,
        campaign_metadata: Dict[str, Any],
        user_data: pd.DataFrame,
        filter_config: Optional[Dict[str, Any]] = None,
        deploy_options: Optional[Dict[str, bool]] = None
    ) -> Dict[str, Any]:
        """Create complete RB1 campaign with filtering and deployment"""
        
        # Generate campaign code
        campaign_code = await self.code_service.generate_next_rb1_code()
        campaign_metadata['campaign_code'] = campaign_code
        
        # Apply filters if specified
        if filter_config:
            filtered_data, filter_stats = self.data_processor.apply_filters_to_data(
                user_data, filter_config
            )
        else:
            filtered_data = user_data
            filter_stats = {"initial_count": len(user_data), "final_count": len(user_data)}
        
        # Set default deployment options
        if deploy_options is None:
            deploy_options = {
                "deploy_metadata": True,
                "deploy_targeting": True,
                "deploy_users": True,
                "deploy_offlimit": True
            }
        
        # Deploy campaign
        deployment_result = self.deployment_service.deploy_rb1_campaign(
            campaign_code, campaign_metadata, filtered_data, deploy_options
        )
        
        return {
            "campaign_code": campaign_code,
            "campaign_type": "RB1",
            "filter_stats": filter_stats,
            "deployment_result": deployment_result,
            "success": deployment_result["success"]
        }
    
    async def create_rb3_campaign(
        self,
        campaign_metadata: Dict[str, Any],
        user_data: pd.DataFrame,
        filter_config: Optional[Dict[str, Any]] = None,
        deploy_options: Optional[Dict[str, bool]] = None
    ) -> Dict[str, Any]:
        """Create complete RB3 campaign with filtering and deployment"""
        
        # Generate campaign code and XLS code
        campaign_code = await self.code_service.generate_next_rb1_code()  # RB3 uses same format
        xls_code = await self.code_service.generate_next_rb3_xls_code()
        
        campaign_metadata['campaign_code'] = campaign_code
        campaign_metadata['xls_ow_id'] = xls_code
        
        # Apply filters if specified
        if filter_config:
            filtered_data, filter_stats = self.data_processor.apply_filters_to_data(
                user_data, filter_config
            )
        else:
            filtered_data = user_data
            filter_stats = {"initial_count": len(user_data), "final_count": len(user_data)}
        
        # Set default deployment options
        if deploy_options is None:
            deploy_options = {
                "deploy_metadata": True,
                "deploy_targeting": True,
                "deploy_users": True
            }
        
        # Deploy campaign
        deployment_result = self.deployment_service.deploy_rb3_campaign(
            campaign_code, campaign_metadata, filtered_data, deploy_options
        )
        
        return {
            "campaign_code": campaign_code,
            "xls_ow_id": xls_code,
            "campaign_type": "RB3",
            "filter_stats": filter_stats,
            "deployment_result": deployment_result,
            "success": deployment_result["success"]
        }

# Global instance
campaign_service = CampaignService() 