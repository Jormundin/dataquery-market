"""
File Upload and Processing Service for DataQuery Pro

Handles Excel, CSV, and Parquet file uploads with IIN extraction and validation.
Replicates the file upload functionality from the original market.py.
"""

import os
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import tempfile
import shutil
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class FileUploadService:
    """Service for handling file uploads and processing"""
    
    def __init__(self):
        # Create uploads directory if it doesn't exist
        self.upload_dir = Path("uploads")
        self.upload_dir.mkdir(exist_ok=True)
        
        # Supported file types
        self.supported_extensions = {'.xlsx', '.xls', '.csv', '.parquet'}
        
        # Maximum file size (50MB)
        self.max_file_size = 50 * 1024 * 1024
        
        # IIN validation pattern (12 digits)
        self.iin_length = 12
        
    def validate_file(self, file_path: str, original_filename: str) -> Dict[str, Any]:
        """Validate uploaded file"""
        result = {
            "valid": True,
            "errors": [],
            "file_info": {}
        }
        
        try:
            # Check file extension
            file_ext = Path(original_filename).suffix.lower()
            if file_ext not in self.supported_extensions:
                result["valid"] = False
                result["errors"].append(f"Неподдерживаемый формат файла: {file_ext}. Поддерживаются: {', '.join(self.supported_extensions)}")
                return result
            
            # Check file size
            file_size = os.path.getsize(file_path)
            if file_size > self.max_file_size:
                result["valid"] = False
                result["errors"].append(f"Файл слишком большой: {file_size / 1024 / 1024:.1f}MB. Максимум: {self.max_file_size / 1024 / 1024}MB")
                return result
            
            result["file_info"] = {
                "size": file_size,
                "extension": file_ext,
                "filename": original_filename
            }
            
            logger.info(f"File validation passed: {original_filename} ({file_size} bytes)")
            return result
            
        except Exception as e:
            logger.error(f"Error validating file {original_filename}: {e}")
            result["valid"] = False
            result["errors"].append(f"Ошибка валидации файла: {str(e)}")
            return result
    
    def detect_iin_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect IIN column in the dataframe"""
        
        # Common IIN column names
        iin_candidates = [
            'IIN', 'iin', 'ИИН', 'иин',
            'ID', 'id', 'ID_NUMBER', 'id_number',
            'CUSTOMER_ID', 'customer_id', 'CLIENT_ID', 'client_id',
            'P_SID', 'p_sid'
        ]
        
        # First check for exact column name matches
        for candidate in iin_candidates:
            if candidate in df.columns:
                # Validate that this column contains IIN-like values
                if self._validate_iin_column(df[candidate]):
                    logger.info(f"IIN column detected by name: {candidate}")
                    return candidate
        
        # Check all columns for IIN-like content
        for column in df.columns:
            if self._validate_iin_column(df[column]):
                logger.info(f"IIN column detected by content: {column}")
                return column
        
        logger.warning("No IIN column detected automatically")
        return None
    
    def _validate_iin_column(self, series: pd.Series) -> bool:
        """Validate if a series contains IIN-like values"""
        
        # Convert to string and clean
        clean_series = series.astype(str).str.strip()
        
        # Remove NaN/null values
        non_null_series = clean_series[clean_series != 'nan']
        
        if len(non_null_series) == 0:
            return False
        
        # Check if at least 80% of values are 12-digit numbers
        valid_iins = 0
        for value in non_null_series.head(100):  # Check first 100 values for performance
            if len(value) == self.iin_length and value.isdigit():
                valid_iins += 1
        
        validation_ratio = valid_iins / min(len(non_null_series), 100)
        return validation_ratio >= 0.8
    
    def load_file_data(self, file_path: str, file_type: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Load data from uploaded file"""
        
        stats = {
            "rows_loaded": 0,
            "columns_detected": [],
            "file_type": file_type,
            "load_time": 0
        }
        
        start_time = datetime.now()
        
        try:
            if file_type in ['.xlsx', '.xls']:
                # Load Excel file
                df = pd.read_excel(file_path, engine='openpyxl' if file_type == '.xlsx' else 'xlrd')
                
            elif file_type == '.csv':
                # Try different encodings for CSV
                encodings = ['utf-8', 'cp1251', 'windows-1251', 'iso-8859-1']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        logger.info(f"CSV loaded successfully with encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    raise ValueError("Не удалось загрузить CSV файл с поддерживаемыми кодировками")
            
            elif file_type == '.parquet':
                # Load Parquet file
                df = pd.read_parquet(file_path)
            
            else:
                raise ValueError(f"Неподдерживаемый тип файла: {file_type}")
            
            # Clean column names
            df.columns = df.columns.astype(str).str.strip()
            
            stats["rows_loaded"] = len(df)
            stats["columns_detected"] = list(df.columns)
            stats["load_time"] = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"File loaded successfully: {stats['rows_loaded']} rows, {len(stats['columns_detected'])} columns")
            
            return df, stats
            
        except Exception as e:
            logger.error(f"Error loading file: {e}")
            raise ValueError(f"Ошибка загрузки файла: {str(e)}")
    
    def extract_iins_from_file(
        self, 
        file_path: str, 
        file_type: str,
        iin_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract IINs from uploaded file"""
        
        try:
            # Load file data
            df, load_stats = self.load_file_data(file_path, file_type)
            
            # Auto-detect IIN column if not specified
            if not iin_column:
                iin_column = self.detect_iin_column(df)
                if not iin_column:
                    return {
                        "success": False,
                        "message": "Не удалось автоматически определить колонку с IIN. Укажите колонку вручную.",
                        "columns_detected": load_stats["columns_detected"],
                        "sample_data": df.head(5).to_dict('records')
                    }
            
            # Validate IIN column exists
            if iin_column not in df.columns:
                return {
                    "success": False,
                    "message": f"Колонка '{iin_column}' не найдена в файле.",
                    "columns_detected": load_stats["columns_detected"],
                    "sample_data": df.head(5).to_dict('records')
                }
            
            # Extract and clean IINs
            raw_iins = df[iin_column].astype(str).str.strip()
            
            # Filter valid IINs (12 digits)
            valid_iins = []
            validation_errors = []
            
            for idx, iin in enumerate(raw_iins):
                if pd.isna(iin) or iin == 'nan' or iin == '':
                    continue
                
                # Clean IIN (remove non-digits)
                clean_iin = ''.join(filter(str.isdigit, iin))
                
                if len(clean_iin) == self.iin_length:
                    valid_iins.append(clean_iin)
                else:
                    validation_errors.append(f"Строка {idx + 1}: '{iin}' не является корректным IIN")
            
            # Remove duplicates while preserving order
            unique_iins = list(dict.fromkeys(valid_iins))
            
            # Prepare sample data (first 5 rows)
            sample_data = df.head(5).to_dict('records')
            
            result = {
                "success": True,
                "message": f"Успешно извлечено {len(unique_iins)} уникальных IIN из {len(valid_iins)} корректных записей",
                "filename": os.path.basename(file_path),
                "file_type": file_type,
                "rows_processed": len(df),
                "columns_detected": load_stats["columns_detected"],
                "iin_column": iin_column,
                "iins_extracted": len(unique_iins),
                "iins": unique_iins,
                "sample_data": sample_data,
                "validation_errors": validation_errors[:10],  # Show first 10 errors
                "load_stats": load_stats
            }
            
            # Log statistics
            logger.info(f"IIN extraction completed: {len(unique_iins)} unique IINs from {len(df)} rows")
            if validation_errors:
                logger.warning(f"Found {len(validation_errors)} validation errors")
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting IINs from file: {e}")
            return {
                "success": False,
                "message": f"Ошибка обработки файла: {str(e)}",
                "validation_errors": [str(e)]
            }
    
    def save_uploaded_file(self, file_content: bytes, filename: str) -> str:
        """Save uploaded file to disk"""
        
        try:
            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = f"{timestamp}_{filename}"
            file_path = self.upload_dir / safe_filename
            
            # Write file
            with open(file_path, 'wb') as f:
                f.write(file_content)
            
            logger.info(f"File saved: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error saving file {filename}: {e}")
            raise ValueError(f"Ошибка сохранения файла: {str(e)}")
    
    def cleanup_old_files(self, hours: int = 24):
        """Clean up old uploaded files"""
        
        try:
            current_time = datetime.now()
            deleted_count = 0
            
            for file_path in self.upload_dir.glob("*"):
                if file_path.is_file():
                    file_age = current_time - datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    if file_age.total_seconds() > hours * 3600:
                        file_path.unlink()
                        deleted_count += 1
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old files older than {hours} hours")
                
        except Exception as e:
            logger.warning(f"Error during file cleanup: {e}")
    
    def process_file_with_filters(
        self,
        file_path: str,
        iin_column: str,
        filter_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process file and apply filters using campaign service"""
        
        try:
            # First extract IINs from file
            extraction_result = self.extract_iins_from_file(
                file_path, 
                Path(file_path).suffix.lower(),
                iin_column
            )
            
            if not extraction_result["success"]:
                return extraction_result
            
            original_iins = extraction_result["iins"]
            
            # If no filters specified, return extracted IINs
            if not filter_config:
                return {
                    "success": True,
                    "message": f"Обработано {len(original_iins)} IIN без применения фильтров",
                    "original_count": len(original_iins),
                    "processed_count": len(original_iins),
                    "filtered_count": len(original_iins),
                    "iins": original_iins,
                    "filter_stats": {"no_filters_applied": True}
                }
            
            # Apply filters using campaign service
            from campaign_service import campaign_service
            
            # Create a mock dataframe for filtering
            iin_df = pd.DataFrame({'IIN': original_iins})
            
            # Apply filters
            filtered_df, filter_stats = campaign_service.data_processor.apply_filters_to_data(
                iin_df, filter_config
            )
            
            filtered_iins = filtered_df['IIN'].tolist() if len(filtered_df) > 0 else []
            
            return {
                "success": True,
                "message": f"Обработано {len(original_iins)} IIN, после фильтрации осталось {len(filtered_iins)}",
                "original_count": len(original_iins),
                "processed_count": len(original_iins),
                "filtered_count": len(filtered_iins),
                "iins": filtered_iins,
                "filter_stats": filter_stats
            }
            
        except Exception as e:
            logger.error(f"Error processing file with filters: {e}")
            return {
                "success": False,
                "message": f"Ошибка обработки файла с фильтрами: {str(e)}"
            }

# Global instance
file_upload_service = FileUploadService()