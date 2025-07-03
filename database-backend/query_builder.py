import re
from typing import Dict, List, Any, Optional
from database import is_table_allowed, get_table_columns, is_table_allowed_case_insensitive, get_table_columns_case_insensitive

class QueryBuilder:
    """Build safe SQL queries from frontend requests"""
    
    def __init__(self):
        self.allowed_operators = {
            'equals': '=',
            'not_equals': '!=',
            'contains': 'LIKE',
            'not_contains': 'NOT LIKE',
            'greater_than': '>',
            'greater_equal': '>=',
            'less_than': '<',
            'less_equal': '<=',
            'is_null': 'IS NULL',
            'is_not_null': 'IS NOT NULL',
            'in': 'IN',
            'not_in': 'NOT IN'
        }
    
    def validate_table_access(self, database_id: str, table_name: str) -> bool:
        """Validate that the table is allowed for access"""
        return is_table_allowed_case_insensitive(database_id, table_name)
    
    def validate_columns(self, database_id: str, table_name: str, columns: List[str]) -> bool:
        """Validate that all requested columns exist in the table"""
        if not columns:
            return True
        
        allowed_columns = get_table_columns_case_insensitive(database_id, table_name)
        allowed_column_names = [col['name'].lower() for col in allowed_columns]
        
        for column in columns:
            if column.lower() not in allowed_column_names:
                return False
        return True
    
    def sanitize_identifier(self, identifier: str) -> str:
        """Sanitize SQL identifiers (table/column names)"""
        # For Oracle schema.table names, allow dots
        if '.' in identifier:
            # Split schema.table and validate each part
            parts = identifier.split('.')
            if len(parts) != 2:
                raise ValueError(f"Invalid schema.table format: {identifier}")
            
            schema, table = parts
            # Validate each part separately
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema):
                raise ValueError(f"Invalid schema name: {schema}")
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
                raise ValueError(f"Invalid table name: {table}")
            
            return f"{schema.upper()}.{table.upper()}"
        else:
            # Single identifier (column name)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
                raise ValueError(f"Invalid identifier: {identifier}")
            return identifier.upper()
    
    def sanitize_value(self, value: Any, operator: str) -> str:
        """Sanitize values for SQL queries"""
        if value is None:
            return 'NULL'
        
        if operator in ['IS NULL', 'IS NOT NULL']:
            return ''
        
        if isinstance(value, (int, float)):
            return str(value)
        
        if isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            if operator in ['LIKE', 'NOT LIKE']:
                return f"'%{escaped}%'"
            return f"'{escaped}'"
        
        if isinstance(value, list) and operator in ['IN', 'NOT IN']:
            sanitized_values = []
            for v in value:
                if isinstance(v, (int, float)):
                    sanitized_values.append(str(v))
                else:
                    escaped_v = str(v).replace("'", "''")
                    sanitized_values.append(f"'{escaped_v}'")
            return f"({', '.join(sanitized_values)})"
        
        # Default case for other types
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    
    def build_where_clause(self, database_id: str, table_name: str, filters: List[Dict[str, Any]]) -> str:
        """Build WHERE clause from filters"""
        if not filters:
            return ""
        
        # Get allowed columns for validation
        allowed_columns = get_table_columns_case_insensitive(database_id, table_name)
        allowed_column_names = [col['name'].lower() for col in allowed_columns]
        
        conditions = []
        search_conditions = []
        
        for filter_item in filters:
            column = filter_item.get('column', '').lower()
            operator = filter_item.get('operator', 'equals')
            value = filter_item.get('value')
            
            # Validate column
            if column not in allowed_column_names:
                continue
            
            # Validate operator
            if operator not in self.allowed_operators:
                operator = 'equals'
            
            sql_operator = self.allowed_operators[operator]
            sanitized_column = self.sanitize_identifier(column)
            
            if operator in ['is_null', 'is_not_null']:
                condition = f"{sanitized_column} {sql_operator}"
            else:
                sanitized_value = self.sanitize_value(value, sql_operator)
                if sanitized_value:  # Skip if value is empty for non-null operators
                    condition = f"{sanitized_column} {sql_operator} {sanitized_value}"
                else:
                    continue
            
            # If this is a search filter (contains operator), group them with OR
            if operator == 'contains' and len(filters) > 1 and all(f.get('operator') == 'contains' for f in filters):
                search_conditions.append(condition)
            else:
                conditions.append(condition)
        
        # Combine conditions
        final_conditions = []
        if search_conditions:
            # Group search conditions with OR
            final_conditions.append(f"({' OR '.join(search_conditions)})")
        
        final_conditions.extend(conditions)
        
        return " WHERE " + " AND ".join(final_conditions) if final_conditions else ""
    
    def build_order_clause(self, database_id: str, table_name: str, sort_by: Optional[str], sort_order: str = "ASC") -> str:
        """Build ORDER BY clause"""
        if not sort_by:
            return ""
        
        # Validate column exists
        allowed_columns = get_table_columns_case_insensitive(database_id, table_name)
        allowed_column_names = [col['name'].lower() for col in allowed_columns]
        
        if sort_by.lower() not in allowed_column_names:
            return ""
        
        # Validate sort order
        sort_order = sort_order.upper()
        if sort_order not in ['ASC', 'DESC']:
            sort_order = 'ASC'
        
        sanitized_column = self.sanitize_identifier(sort_by)
        return f" ORDER BY {sanitized_column} {sort_order}"
    
    def build_select_clause(self, database_id: str, table_name: str, columns: Optional[List[str]] = None) -> str:
        """Build SELECT clause"""
        if not columns:
            return "SELECT *"
        
        # Validate all columns exist
        if not self.validate_columns(database_id, table_name, columns):
            return "SELECT *"
        
        sanitized_columns = []
        for column in columns:
            sanitized_columns.append(self.sanitize_identifier(column))
        
        return f"SELECT {', '.join(sanitized_columns)}"
    
    def build_query(self, request_data: Dict[str, Any]) -> str:
        """Build complete SQL query from request data"""
        database_id = request_data.get('database_id', '').upper()
        table_name = request_data.get('table', '')
        columns = request_data.get('columns')
        filters = request_data.get('filters', [])
        sort_by = request_data.get('sort_by')
        sort_order = request_data.get('sort_order', 'ASC')
        limit = request_data.get('limit', 100)
        
        # Validate table access
        if not self.validate_table_access(database_id, table_name):
            raise ValueError(f"Access denied to table: {table_name}")
        
        # Build query components
        select_clause = self.build_select_clause(database_id, table_name, columns)
        table_clause = f" FROM {self.sanitize_identifier(table_name)}"
        where_clause = self.build_where_clause(database_id, table_name, filters)
        order_clause = self.build_order_clause(database_id, table_name, sort_by, sort_order)
        
        # Build complete query
        query = select_clause + table_clause + where_clause + order_clause
        
        # Add limit using Oracle ROWNUM
        if limit and limit > 0:
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {int(limit)}"
        
        return query
    
    def build_count_query(self, request_data: Dict[str, Any]) -> str:
        """Build count query for pagination"""
        database_id = request_data.get('database_id', '').upper()
        table_name = request_data.get('table', '')
        filters = request_data.get('filters', [])
        
        # Validate table access
        if not self.validate_table_access(database_id, table_name):
            raise ValueError(f"Access denied to table: {table_name}")
        
        table_clause = f" FROM {self.sanitize_identifier(table_name)}"
        where_clause = self.build_where_clause(database_id, table_name, filters)
        
        return f"SELECT COUNT(*){table_clause}{where_clause}" 