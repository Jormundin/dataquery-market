"""
Parquet Data Service for DataQuery Pro

Handles loading, caching, and managing parquet files from the Databases/ directory.
Provides the data foundation for stop lists, filtering, and targeting functionality
migrated from market.py.
"""

import os
import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from functools import lru_cache
import json
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParquetDataService:
    """Service for managing parquet data files"""
    
    def __init__(self, base_path: str = "Databases"):
        self.base_path = Path(base_path)
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = timedelta(hours=1)  # Cache for 1 hour
        
        # Known parquet files from market.py
        self.known_datasets = {
            # Stop/Black Lists
            'ACRM_DW.RB_BLACK_LIST@ACRM': {
                'file': 'ACRM_DW.RB_BLACK_LIST@ACRM.parquet',
                'description': 'ACRM Black List',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'dssb_de.dim_clients_black_list': {
                'file': 'dssb_de.dim_clients_black_list.parquet',
                'description': 'Clients Black List',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK': {
                'file': 'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK.parquet',
                'description': 'Halyk Job Users',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK': {
                'file': 'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK.parquet',
                'description': 'Bloggers List',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'dssb_app.not_recommend_credits': {
                'file': 'dssb_app.not_recommend_credits.parquet',
                'description': 'Not Recommend Credits',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'DSSB_OCDS.mb11_global_control': {
                'file': 'DSSB_OCDS.mb11_global_control.parquet',
                'description': 'Global Control List',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            'BL_No_worker': {
                'file': 'BL_No_worker.parquet',
                'description': 'No Worker Black List',
                'category': 'blacklist',
                'columns': ['IIN']
            },
            
            # ABC Models
            'dssb_app.abc_nbo_only': {
                'file': 'dssb_app.abc_nbo_only.parquet',
                'description': 'ABC NBO Only',
                'category': 'abc_model',
                'columns': ['IIN']
            },
            'dssb_app.abc_ptb_models': {
                'file': 'dssb_app.abc_ptb_models.parquet',
                'description': 'ABC PTB Models',
                'category': 'abc_model',
                'columns': ['IIN']
            },
            'dssb_app.abc_nbo_and_market': {
                'file': 'dssb_app.abc_nbo_and_market.parquet',
                'description': 'ABC NBO and Market',
                'category': 'abc_model',
                'columns': ['IIN']
            },
            
            # Push/Device Data
            'DSSB_SE.UCS_HB_PUSH': {
                'file': 'DSSB_SE.UCS_HB_PUSH.parquet',
                'description': 'Push Notifications Data',
                'category': 'push',
                'columns': ['IIN']
            },
            'DSSB_DE.UCS_PUSH_OFF': {
                'file': 'DSSB_DE.UCS_PUSH_OFF.parquet',
                'description': 'Push Off Events',
                'category': 'push',
                'columns': ['IIN', 'EVENTDESCRIPTION']
            },
            'dssb_dm.hb_sessions_fl': {
                'file': 'dssb_dm.hb_sessions_fl.parquet',
                'description': 'HB Sessions (Device Data)',
                'category': 'device',
                'columns': ['CLIENT_IIN', 'OPERATIONSYSTEM']
            },
            
            # Analytics
            'dssb_dev.dssb_push_analytics': {
                'file': 'dssb_dev.dssb_push_analytics.parquet',
                'description': 'Push Analytics (MAU)',
                'category': 'analytics',
                'columns': ['IIN']
            },
            'MAU': {
                'file': 'MAU.parquet',
                'description': 'Monthly Active Users',
                'category': 'analytics',
                'columns': ['IIN']
            },
            
            # Products
            'final': {
                'file': 'final.parquet',
                'description': 'Product Data',
                'category': 'products',
                'columns': ['IIN', 'sku_level1']
            },
            'dssb_app.products_per_fl_prod': {
                'file': 'dssb_app.products_per_fl_prod.parquet',
                'description': 'Products per FL Production',
                'category': 'products',
                'columns': ['IIN']
            }
        }
    
    def _is_cache_valid(self, dataset_name: str) -> bool:
        """Check if cached data is still valid"""
        if dataset_name not in self._cache_timestamps:
            return False
        
        cache_time = self._cache_timestamps[dataset_name]
        return datetime.now() - cache_time < self._cache_ttl
    
    def _get_file_path(self, dataset_name: str) -> Path:
        """Get full file path for a dataset"""
        if dataset_name not in self.known_datasets:
            raise ValueError(f"Unknown dataset: {dataset_name}")
        
        filename = self.known_datasets[dataset_name]['file']
        return self.base_path / filename
    
    def file_exists(self, dataset_name: str) -> bool:
        """Check if a parquet file exists"""
        try:
            file_path = self._get_file_path(dataset_name)
            return file_path.exists()
        except ValueError:
            return False
    
    def get_available_datasets(self) -> Dict[str, Dict[str, Any]]:
        """Get list of available datasets with their status"""
        available = {}
        
        for dataset_name, info in self.known_datasets.items():
            file_path = self._get_file_path(dataset_name)
            file_exists = file_path.exists()
            
            available[dataset_name] = {
                **info,
                'available': file_exists,
                'file_path': str(file_path),
                'cached': dataset_name in self._cache and self._is_cache_valid(dataset_name)
            }
        
        return available
    
    def get_datasets_by_category(self, category: str) -> List[str]:
        """Get dataset names by category"""
        return [
            name for name, info in self.known_datasets.items()
            if info['category'] == category
        ]
    
    def load_dataset(self, dataset_name: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """Load a parquet dataset with caching"""
        # Check cache first
        if use_cache and dataset_name in self._cache and self._is_cache_valid(dataset_name):
            logger.info(f"Loading {dataset_name} from cache")
            return self._cache[dataset_name]
        
        # Get file path
        try:
            file_path = self._get_file_path(dataset_name)
        except ValueError as e:
            logger.error(f"Unknown dataset: {dataset_name}")
            return None
        
        # Check if file exists
        if not file_path.exists():
            logger.warning(f"Parquet file not found: {file_path}")
            return self._create_mock_dataset(dataset_name)
        
        try:
            # Load the parquet file
            logger.info(f"Loading parquet file: {file_path}")
            df = pd.read_parquet(file_path)
            
            # Validate expected columns
            expected_columns = self.known_datasets[dataset_name]['columns']
            missing_columns = set(expected_columns) - set(df.columns)
            if missing_columns:
                logger.warning(f"Missing expected columns in {dataset_name}: {missing_columns}")
            
            # Cache the data
            if use_cache:
                self._cache[dataset_name] = df
                self._cache_timestamps[dataset_name] = datetime.now()
            
            logger.info(f"Successfully loaded {dataset_name}: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error loading parquet file {file_path}: {e}")
            return self._create_mock_dataset(dataset_name)
    
    def _create_mock_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Create mock dataset for testing when files don't exist"""
        logger.info(f"Creating mock dataset for {dataset_name}")
        
        info = self.known_datasets[dataset_name]
        columns = info['columns']
        
        # Create empty DataFrame with expected columns
        mock_data = {}
        for col in columns:
            if col in ['IIN', 'CLIENT_IIN']:
                mock_data[col] = []  # Empty IIN list
            elif col == 'OPERATIONSYSTEM':
                mock_data[col] = []  # Empty OS list
            elif col == 'EVENTDESCRIPTION':
                mock_data[col] = []  # Empty event descriptions
            elif col == 'sku_level1':
                mock_data[col] = []  # Empty product list
            else:
                mock_data[col] = []  # Default empty
        
        return pd.DataFrame(mock_data)
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific dataset"""
        if dataset_name not in self.known_datasets:
            return None
        
        info = self.known_datasets[dataset_name].copy()
        file_path = self._get_file_path(dataset_name)
        
        info.update({
            'available': file_path.exists(),
            'file_path': str(file_path),
            'cached': dataset_name in self._cache and self._is_cache_valid(dataset_name),
            'cache_timestamp': self._cache_timestamps.get(dataset_name),
            'file_size': file_path.stat().st_size if file_path.exists() else 0
        })
        
        # Add data info if loaded
        if dataset_name in self._cache:
            df = self._cache[dataset_name]
            info.update({
                'row_count': len(df),
                'column_count': len(df.columns),
                'actual_columns': list(df.columns)
            })
        
        return info
    
    def clear_cache(self, dataset_name: Optional[str] = None):
        """Clear cache for specific dataset or all datasets"""
        if dataset_name:
            if dataset_name in self._cache:
                del self._cache[dataset_name]
                del self._cache_timestamps[dataset_name]
                logger.info(f"Cleared cache for {dataset_name}")
        else:
            self._cache.clear()
            self._cache_timestamps.clear()
            logger.info("Cleared all cache")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'cached_datasets': list(self._cache.keys()),
            'cache_size': len(self._cache),
            'cache_ttl_hours': self._cache_ttl.total_seconds() / 3600,
            'timestamps': {
                name: timestamp.isoformat()
                for name, timestamp in self._cache_timestamps.items()
            }
        }
    
    # Market.py Integration Methods
    def get_blacklist_iins(self, blacklist_tables: List[str]) -> List[str]:
        """Get all IINs from specified blacklist tables (market.py integration)"""
        all_iins = set()
        
        for table in blacklist_tables:
            if table in self.known_datasets:
                df = self.load_dataset(table)
                if df is not None and not df.empty:
                    # Handle different IIN column names
                    iin_col = 'IIN' if 'IIN' in df.columns else 'CLIENT_IIN'
                    if iin_col in df.columns:
                        iins = df[iin_col].dropna().astype(str).tolist()
                        all_iins.update(iins)
                        logger.info(f"Added {len(iins)} IINs from {table}")
        
        return list(all_iins)
    
    def get_device_filtered_iins(self, selected_devices: List[str]) -> List[str]:
        """Get IINs filtered by device type (market.py integration)"""
        df = self.load_dataset('dssb_dm.hb_sessions_fl')
        if df is None or df.empty:
            return []
        
        # Filter by operating system
        if 'OPERATIONSYSTEM' in df.columns:
            filtered_df = df[df['OPERATIONSYSTEM'].isin(selected_devices)]
            iin_col = 'CLIENT_IIN' if 'CLIENT_IIN' in filtered_df.columns else 'IIN'
            if iin_col in filtered_df.columns:
                return filtered_df[iin_col].dropna().astype(str).unique().tolist()
        
        return []
    
    def get_push_filtered_iins(self, selected_streams: List[str]) -> List[str]:
        """Get IINs filtered by push preferences (market.py integration)"""
        df = self.load_dataset('DSSB_DE.UCS_PUSH_OFF')
        if df is None or df.empty:
            return []
        
        if 'EVENTDESCRIPTION' in df.columns:
            filtered_df = df[df['EVENTDESCRIPTION'].isin(selected_streams)]
            iin_col = 'IIN' if 'IIN' in filtered_df.columns else 'CLIENT_IIN'
            if iin_col in filtered_df.columns:
                return filtered_df[iin_col].dropna().astype(str).unique().tolist()
        
        return []
    
    def get_mau_iins(self) -> List[str]:
        """Get MAU covered IINs (market.py integration)"""
        df = self.load_dataset('MAU')
        if df is None or df.empty:
            return []
        
        if 'IIN' in df.columns:
            return df['IIN'].dropna().astype(str).unique().tolist()
        
        return []
    
    def get_product_iins(self, selected_products: List[str]) -> List[str]:
        """Get IINs by product selection (market.py integration)"""
        df = self.load_dataset('final')
        if df is None or df.empty:
            return []
        
        if 'sku_level1' in df.columns and 'IIN' in df.columns:
            filtered_df = df[df['sku_level1'].isin(selected_products)]
            return filtered_df['IIN'].dropna().astype(str).unique().tolist()
        
        return []

# Global instance
parquet_service = ParquetDataService() 