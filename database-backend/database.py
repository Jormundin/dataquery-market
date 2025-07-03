import os
import cx_Oracle
from typing import List, Dict, Optional
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Hardcoded list of tables that frontend has access to
ALLOWED_TABLES = {
    'DSSB_APP': {
        'DSSB_DM.RB_CLIENTS': {
            'description': 'Информация о клиентах',
            'columns': [
                {'name': 'SNAPSHOT_DATE', 'type': 'DATE', 'description': 'DATE'},
                {'name': 'OCRM_DWH_ID', 'type': 'NUMBER', 'description': 'ID'},
                {'name': 'GM_SYSTEM_CODE', 'type': 'VARCHAR2', 'description': 'ID'},
                {'name': 'IIN_BIN', 'type': 'NUMBER', 'description': 'ID'},
                {'name': 'OCRM_ID', 'type': 'VARCHAR2', 'description': 'ID'},
                {'name': 'LAST_NAME', 'type': 'VARCHAR2', 'description': 'NAME'},
                {'name': 'FIRST_NAME', 'type': 'VARCHAR2', 'description': 'NAME'},
                {'name': 'MIDDLE_NAME', 'type': 'VARCHAR2', 'description': 'NAME'},
                {'name': 'LONG_NAME', 'type': 'VARCHAR2', 'description': 'NAME'},
                {'name': 'SEX_CODE', 'type': 'VARCHAR2', 'description': 'Статус'},
                {'name': 'MARITAL_STATUS', 'type': 'VARCHAR2', 'description': 'Статус'},
                {'name': 'BIRTH_DATE', 'type': 'DATE', 'description': 'DATE'},
                {'name': 'CITY_RESIDENCE', 'type': 'VARCHAR2', 'description': 'NAME'},
                {'name': 'AGE', 'type': 'DATE', 'description': 'DATE'},
            ]
    },
        'DSSB_APP.SoftCollection_theories': {
            'description': 'Реестр кампаний Soft Collection (одна запись на кампанию)',
            'columns': [
                {'name': 'THEORY_ID', 'type': 'VARCHAR2', 'description': 'Уникальный ID кампании'},
                {'name': 'THEORY_NAME', 'type': 'VARCHAR2', 'description': 'Название кампании'},
                {'name': 'THEORY_DESCRIPTION', 'type': 'VARCHAR2', 'description': 'Описание кампании'},
                {'name': 'LOAD_DATE', 'type': 'DATE', 'description': 'Дата создания записи'},
                {'name': 'THEORY_START_DATE', 'type': 'DATE', 'description': 'Дата начала кампании'},
                {'name': 'THEORY_END_DATE', 'type': 'DATE', 'description': 'Дата окончания кампании'},
                {'name': 'USER_COUNT', 'type': 'NUMBER', 'description': 'Количество пользователей в кампании'},
                {'name': 'CREATED_BY', 'type': 'VARCHAR2', 'description': 'Кто создал кампанию'},
            ]
        },
        'DSSB_APP.SC_local_control': {
            'description': 'Контрольная группа пользователей из стратификации',
            'columns': [
                {'name': 'IIN', 'type': 'VARCHAR2', 'description': 'Индивидуальный идентификационный номер'},
                {'name': 'THEORY_ID', 'type': 'VARCHAR2', 'description': 'ID кампании'},
                {'name': 'DATE_START', 'type': 'DATE', 'description': 'Дата начала кампании'},
                {'name': 'DATE_END', 'type': 'DATE', 'description': 'Дата окончания кампании'},
                {'name': 'INSERT_DATETIME', 'type': 'DATE', 'description': 'Время вставки записи'},
                {'name': 'TAB1', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 1'},
                {'name': 'TAB2', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 2'},
                {'name': 'TAB3', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 3'},
                {'name': 'TAB4', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 4'},
                {'name': 'TAB5', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 5'},
            ]
        },
        'DSSB_APP.SC_local_target': {
            'description': 'Целевые группы пользователей из стратификации',
            'columns': [
                {'name': 'IIN', 'type': 'VARCHAR2', 'description': 'Индивидуальный идентификационный номер'},
                {'name': 'THEORY_ID', 'type': 'VARCHAR2', 'description': 'ID кампании'},
                {'name': 'DATE_START', 'type': 'DATE', 'description': 'Дата начала кампании'},
                {'name': 'DATE_END', 'type': 'DATE', 'description': 'Дата окончания кампании'},
                {'name': 'INSERT_DATETIME', 'type': 'DATE', 'description': 'Время вставки записи'},
                {'name': 'TAB1', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 1'},
                {'name': 'TAB2', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 2'},
                {'name': 'TAB3', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 3'},
                {'name': 'TAB4', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 4'},
                {'name': 'TAB5', 'type': 'VARCHAR2', 'description': 'Дополнительное поле 5'},
            ]
        }
    }
}

def get_connection_DSSB_APP():
    """Establish a connection to the DSSB_APP database"""
    try:
        # Get database configuration from environment variables
        oracle_host = os.getenv('ORACLE_HOST', '')
        oracle_port = os.getenv('ORACLE_PORT', '1521')
        oracle_sid = os.getenv('ORACLE_SID', '')
        oracle_user = os.getenv('ORACLE_USER', '')
        oracle_password = os.getenv('ORACLE_PASSWORD', '')
        
        if not all([oracle_host, oracle_sid, oracle_user, oracle_password]):
            raise ValueError("Missing required database environment variables. Please check ORACLE_HOST, ORACLE_SID, ORACLE_USER, and ORACLE_PASSWORD.")
        
        dsn = cx_Oracle.makedsn(oracle_host, oracle_port, sid=oracle_sid)
        return cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
    except cx_Oracle.Error as e:
        print(f"Database connection error: {str(e)}")
        raise
    except ValueError as e:
        print(f"Configuration error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error in database connection: {str(e)}")
        raise

def get_connection_SPSS():
    """Establish a connection to the SPSS database"""
    try:
        # Get SPSS database configuration from environment variables
        spss_host = os.getenv('SPSS_ORACLE_HOST', '')
        spss_port = os.getenv('SPSS_ORACLE_PORT', '1521')
        spss_sid = os.getenv('SPSS_ORACLE_SID', '')
        spss_user = os.getenv('SPSS_ORACLE_USER', '')
        spss_password = os.getenv('SPSS_ORACLE_PASSWORD', '')
        
        if not all([spss_host, spss_sid, spss_user, spss_password]):
            raise ValueError("Missing required SPSS database environment variables. Please check SPSS_ORACLE_HOST, SPSS_ORACLE_SID, SPSS_ORACLE_USER, and SPSS_ORACLE_PASSWORD.")
        
        dsn = cx_Oracle.makedsn(spss_host, spss_port, sid=spss_sid)
        return cx_Oracle.connect(user=spss_user, password=spss_password, dsn=dsn)
    except cx_Oracle.Error as e:
        print(f"SPSS Database connection error: {str(e)}")
        raise
    except ValueError as e:
        print(f"SPSS Configuration error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error in SPSS database connection: {str(e)}")
        raise

def get_connection_DSSB_OCDS():
    """Establish a connection to the DSSB_OCDS database (for campaign management)"""
    try:
        # Get DSSB_OCDS database configuration from environment variables
        dssb_ocds_host = os.getenv('DSSB_OCDS_ORACLE_HOST', '')
        dssb_ocds_port = os.getenv('DSSB_OCDS_ORACLE_PORT', '1521')
        dssb_ocds_sid = os.getenv('DSSB_OCDS_ORACLE_SID', '')
        dssb_ocds_user = os.getenv('DSSB_OCDS_ORACLE_USER', '')
        dssb_ocds_password = os.getenv('DSSB_OCDS_ORACLE_PASSWORD', '')
        
        if not all([dssb_ocds_host, dssb_ocds_sid, dssb_ocds_user, dssb_ocds_password]):
            raise ValueError("Missing required DSSB_OCDS database environment variables. Please check DSSB_OCDS_ORACLE_HOST, DSSB_OCDS_ORACLE_SID, DSSB_OCDS_ORACLE_USER, and DSSB_OCDS_ORACLE_PASSWORD.")
        
        dsn = cx_Oracle.makedsn(dssb_ocds_host, dssb_ocds_port, sid=dssb_ocds_sid)
        return cx_Oracle.connect(user=dssb_ocds_user, password=dssb_ocds_password, dsn=dsn)
    except cx_Oracle.Error as e:
        print(f"DSSB_OCDS Database connection error: {str(e)}")
        raise
    except ValueError as e:
        print(f"DSSB_OCDS Configuration error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error in DSSB_OCDS database connection: {str(e)}")
        raise



def get_connection_ED_OCDS():
    """Establish a connection to the ED_OCDS database (for campaign tracking)"""
    try:
        # Get ED_OCDS database configuration from environment variables
        ed_ocds_host = os.getenv('ED_OCDS_ORACLE_HOST', '')
        ed_ocds_port = os.getenv('ED_OCDS_ORACLE_PORT', '1521')
        ed_ocds_sid = os.getenv('ED_OCDS_ORACLE_SID', '')
        ed_ocds_user = os.getenv('ED_OCDS_ORACLE_USER', '')
        ed_ocds_password = os.getenv('ED_OCDS_ORACLE_PASSWORD', '')
        
        if not all([ed_ocds_host, ed_ocds_sid, ed_ocds_user, ed_ocds_password]):
            raise ValueError("Missing required ED_OCDS database environment variables. Please check ED_OCDS_ORACLE_HOST, ED_OCDS_ORACLE_SID, ED_OCDS_ORACLE_USER, and ED_OCDS_ORACLE_PASSWORD.")
        
        dsn = cx_Oracle.makedsn(ed_ocds_host, ed_ocds_port, sid=ed_ocds_sid)
        return cx_Oracle.connect(user=ed_ocds_user, password=ed_ocds_password, dsn=dsn)
    except cx_Oracle.Error as e:
        print(f"ED_OCDS Database connection error: {str(e)}")
        raise
    except ValueError as e:
        print(f"ED_OCDS Configuration error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error in ED_OCDS database connection: {str(e)}")
        raise

def test_connection() -> Dict:
    """Test database connection and return status"""
    try:
        conn = get_connection_DSSB_APP()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Соединение с базой данных DSSB_APP установлено успешно",
            "connected": True
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Ошибка подключения к базе данных DSSB_APP: {str(e)}",
            "connected": False
        }

def test_spss_connection() -> Dict:
    """Test SPSS database connection and return status"""
    try:
        conn = get_connection_SPSS()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Соединение с базой данных SPSS установлено успешно",
            "connected": True
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Ошибка подключения к базе данных SPSS: {str(e)}",
            "connected": False
        }

def test_dssb_ocds_connection() -> Dict:
    """Test DSSB_OCDS database connection and return status"""
    try:
        conn = get_connection_DSSB_OCDS()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Соединение с базой данных DSSB_OCDS установлено успешно",
            "connected": True
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Ошибка подключения к базе данных DSSB_OCDS: {str(e)}",
            "connected": False
        }



def test_ed_ocds_connection() -> Dict:
    """Test ED_OCDS database connection and return status"""
    try:
        conn = get_connection_ED_OCDS()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Соединение с базой данных ED_OCDS установлено успешно",
            "connected": True
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Ошибка подключения к базе данных ED_OCDS: {str(e)}",
            "connected": False
        }

def test_all_connections() -> Dict:
    """Test all database connections and return comprehensive status"""
    dssb_result = test_connection()
    spss_result = test_spss_connection()
    dssb_ocds_result = test_dssb_ocds_connection()
    ed_ocds_result = test_ed_ocds_connection()
    
    # Count successful connections
    successful_connections = sum([
        dssb_result["connected"],
        spss_result["connected"], 
        dssb_ocds_result["connected"],
        ed_ocds_result["connected"]
    ])
    
    total_connections = 4
    
    # Determine overall status
    if successful_connections == total_connections:
        overall_status = "success"
        overall_message = "Все соединения успешны"
    elif successful_connections > 0:
        overall_status = "partial"
        overall_message = f"Работает {successful_connections} из {total_connections} соединений"
    else:
        overall_status = "error"
        overall_message = "Все соединения недоступны"
    
    return {
        "dssb_app": dssb_result,
        "spss": spss_result,
        "dssb_ocds": dssb_ocds_result,
        "ed_ocds": ed_ocds_result,
        "overall_status": overall_status,
        "message": overall_message,
        "successful_connections": successful_connections,
        "total_connections": total_connections
    }

def get_databases() -> List[Dict]:
    """Get list of available databases"""
    return [
        {
            "id": "DSSB_APP",
            "name": "Продуктивная база данных DSSB_APP",
            "description": "Основная корпоративная база данных",
            "status": "active"
        },
        {
            "id": "SPSS",
            "name": "База данных SPSS",
            "description": "База данных для SPSS аналитики",
            "status": "active"
        },
        {
            "id": "DSSB_OCDS",
            "name": "База данных DSSB_OCDS",
            "description": "База данных для управления кампаниями OCDS",
            "status": "active"
        },

        {
            "id": "ED_OCDS",
            "name": "База данных ED_OCDS",
            "description": "База данных для отслеживания кампаний ED OCDS",
            "status": "active"
        }
    ]

def get_tables(database_id: str) -> List[Dict]:
    """Get list of tables for a specific database"""
    if database_id not in ALLOWED_TABLES:
        return []
    
    tables = []
    for table_name, table_info in ALLOWED_TABLES[database_id].items():
        tables.append({
            "name": table_name,
            "description": table_info["description"],
            "columns_count": len(table_info["columns"])
        })
    
    return tables

def get_table_columns(database_id: str, table_name: str) -> List[Dict]:
    """Get columns for a specific table"""
    if database_id not in ALLOWED_TABLES or table_name not in ALLOWED_TABLES[database_id]:
        return []
    
    return ALLOWED_TABLES[database_id][table_name]["columns"]

def get_table_columns_case_insensitive(database_id: str, table_name: str) -> List[Dict]:
    """Get columns for a specific table with case-insensitive table name lookup"""
    # First try exact database_id match
    db_key = None
    for key in ALLOWED_TABLES.keys():
        if key.upper() == database_id.upper():
            db_key = key
            break
    
    if not db_key:
        return []
    
    # Now try case-insensitive table name lookup
    table_key = None
    for key in ALLOWED_TABLES[db_key].keys():
        if key.upper() == table_name.upper():
            table_key = key
            break
    
    if not table_key:
        return []
    
    return ALLOWED_TABLES[db_key][table_key]["columns"]

def is_table_allowed(database_id: str, table_name: str) -> bool:
    """Check if table access is allowed"""
    return database_id in ALLOWED_TABLES and table_name in ALLOWED_TABLES[database_id]

def is_table_allowed_case_insensitive(database_id: str, table_name: str) -> bool:
    """Check if table access is allowed with case-insensitive lookup"""
    # First try exact database_id match
    db_key = None
    for key in ALLOWED_TABLES.keys():
        if key.upper() == database_id.upper():
            db_key = key
            break
    
    if not db_key:
        return False
    
    # Now try case-insensitive table name lookup
    for key in ALLOWED_TABLES[db_key].keys():
        if key.upper() == table_name.upper():
            return True
    
    return False

def execute_query(sql: str, params: Dict = None) -> Dict:
    """Execute SQL query and return results"""
    try:
        conn = get_connection_DSSB_APP()
        cursor = conn.cursor()
        
        # Execute query
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        # Get column names
        columns = [desc[0].lower() for desc in cursor.description] if cursor.description else []
        
        # Fetch results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        data = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                # Handle different Oracle data types
                if isinstance(value, cx_Oracle.LOB):
                    value = value.read() if value else None
                elif hasattr(value, 'isoformat'):  # Date/DateTime
                    value = value.isoformat()
                row_dict[columns[i]] = value
            data.append(row_dict)
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "columns": columns,
            "data": data,
            "row_count": len(data),
            "message": "Запрос выполнен успешно"
        }
        
    except Exception as e:
        print(f"Query execution error: {e}")
        return {
            "success": False,
            "message": f"Database query failed: {str(e)}",
            "error": str(e)
        }

# Theory Management Functions
def get_next_sc_campaign_id():
    """Get next available SC campaign ID in format SC00000001, SC00000002, etc."""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Find the highest SC campaign number
        cursor.execute(r"""
        SELECT NVL(MAX(
            CASE 
                WHEN REGEXP_LIKE(theory_id, '^SC[0-9]{8}\.[0-9]+$') THEN 
                    TO_NUMBER(SUBSTR(theory_id, 3, 8))
                ELSE 0
            END
        ), 0) + 1 as next_campaign_id
        FROM SoftCollection_theories
        """)
        result = cursor.fetchone()
        next_campaign_num = result[0] if result else 1
        
        cursor.close()
        connection.close()
        
        # Format as SC00000001, SC00000002, etc.
        return f"SC{next_campaign_num:08d}"
        
    except Exception as e:
        print(f"Error getting next SC campaign ID: {e}")
        # Return a safe default
        return "SC00000001"

def get_next_theory_id():
    """Get next available theory ID for backward compatibility (numeric format)"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Since theory_id is now VARCHAR2, we need to find the highest numeric base ID
        # This handles both old numeric IDs (1, 2, 3) and new decimal IDs (4.1, 4.2, 4.3)
        # but excludes SC format IDs
        cursor.execute(r"""
        SELECT NVL(MAX(
            CASE 
                WHEN REGEXP_LIKE(theory_id, '^[0-9]+(\.[0-9]+)?$') THEN
                    CASE 
                        WHEN INSTR(theory_id, '.') > 0 THEN 
                            TO_NUMBER(SUBSTR(theory_id, 1, INSTR(theory_id, '.') - 1))
                        ELSE 
                            TO_NUMBER(theory_id)
                    END
                ELSE 0
            END
        ), 0) + 1 
        FROM SoftCollection_theories
        WHERE REGEXP_LIKE(theory_id, '^[0-9]+(\.[0-9]+)?$')
        """)
        result = cursor.fetchone()
        next_id = result[0] if result else 1
        
        cursor.close()
        connection.close()
        
        return int(next_id)
        
    except Exception as e:
        print(f"Error getting next theory ID: {e}")
        # Return a safe default
        return 1

def create_theory_with_custom_id(theory_name, theory_description, theory_start_date, theory_end_date, user_iins, created_by, custom_theory_id):
    """Create a new campaign record with a custom theory ID (for stratification sub-IDs)"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Count unique users (remove duplicates and empty values)
        unique_users = set()
        for iin in user_iins:
            if iin and str(iin).strip():
                unique_users.add(str(iin).strip())
        user_count = len(unique_users)
        
        # Insert single campaign record
        insert_sql = """
        INSERT INTO SoftCollection_theories 
        (theory_id, theory_name, theory_description, load_date, theory_start_date, theory_end_date, user_count, created_by)
        VALUES (:1, :2, :3, SYSDATE, TO_DATE(:4, 'YYYY-MM-DD'), TO_DATE(:5, 'YYYY-MM-DD'), :6, :7)
        """
        
        cursor.execute(insert_sql, (
            custom_theory_id,
            theory_name, 
            theory_description,
            theory_start_date,
            theory_end_date,
            user_count,
            created_by
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "message": f"Campaign '{theory_name}' created successfully with ID {custom_theory_id}",
            "theory_id": custom_theory_id,
            "users_added": user_count
        }
        
    except Exception as e:
        print(f"Error creating campaign with custom ID: {e}")
        return {
            "success": False,
            "message": f"Failed to create campaign: {str(e)}"
        }

def create_theory(theory_name, theory_description, theory_start_date, theory_end_date, user_iins, created_by):
    """Create a new campaign with user assignments (backward compatibility - uses numeric ID)"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Get next theory ID
        theory_id = get_next_theory_id()
        
        # Count unique users (remove duplicates and empty values)
        unique_users = set()
        for iin in user_iins:
            if iin and str(iin).strip():
                unique_users.add(str(iin).strip())
        user_count = len(unique_users)
        
        # Insert single campaign record
        insert_sql = """
        INSERT INTO SoftCollection_theories 
        (theory_id, theory_name, theory_description, load_date, theory_start_date, theory_end_date, user_count, created_by)
        VALUES (:1, :2, :3, SYSDATE, TO_DATE(:4, 'YYYY-MM-DD'), TO_DATE(:5, 'YYYY-MM-DD'), :6, :7)
        """
        
        cursor.execute(insert_sql, (
            theory_id,
            theory_name, 
            theory_description,
            theory_start_date,
            theory_end_date,
            user_count,
            created_by
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "message": f"Campaign '{theory_name}' created successfully",
            "theory_id": theory_id,
            "users_added": user_count
        }
        
    except Exception as e:
        print(f"Error creating campaign: {e}")
        return {
            "success": False,
            "message": f"Failed to create campaign: {str(e)}"
        }

def get_active_theories():
    """Get all campaign records with enhanced sorting for both numeric and SC formats"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Get all campaign records - much simpler now with one record per campaign
        query = r"""
        SELECT 
            theory_id,
            theory_name,
            theory_description,
            TO_CHAR(load_date, 'YYYY-MM-DD') as load_date,
            TO_CHAR(theory_start_date, 'YYYY-MM-DD') as theory_start_date,
            TO_CHAR(theory_end_date, 'YYYY-MM-DD') as theory_end_date,
            user_count,
            CASE WHEN SYSDATE BETWEEN theory_start_date AND theory_end_date THEN 1 ELSE 0 END as is_active,
            created_by
        FROM SoftCollection_theories
        ORDER BY 
            theory_start_date DESC,
            CASE 
                -- SC format sorting (SC00000001.1, SC00000001.2, etc.)
                WHEN REGEXP_LIKE(theory_id, '^SC[0-9]{8}\.[0-9]+$') THEN
                    TO_NUMBER(SUBSTR(theory_id, 3, 8)) * 1000 + TO_NUMBER(SUBSTR(theory_id, INSTR(theory_id, '.') + 1))
                -- Numeric format sorting (1, 2, 3, 1.1, 1.2, etc.)
                WHEN REGEXP_LIKE(theory_id, '^[0-9]+(\.[0-9]+)?$') THEN
                    CASE 
                        WHEN INSTR(theory_id, '.') > 0 THEN 
                            TO_NUMBER(SUBSTR(theory_id, 1, INSTR(theory_id, '.') - 1)) * 1000 + 
                            TO_NUMBER(SUBSTR(theory_id, INSTR(theory_id, '.') + 1))
                        ELSE 
                            TO_NUMBER(theory_id) * 1000
                    END
                -- Other formats go to the end
                ELSE 999999999
            END DESC,
            theory_id DESC
        """
        
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        
        theories = []
        for row in cursor.fetchall():
            theory_dict = dict(zip(columns, row))
            theory_dict['is_active'] = bool(theory_dict['is_active'])
            theories.append(theory_dict)
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "data": theories,
            "message": f"Retrieved {len(theories)} campaigns"
        }
        
    except Exception as e:
        print(f"Error getting campaigns: {e}")
        return {
            "success": False,
            "data": [],
            "message": f"Failed to get campaigns: {str(e)}"
        }

def detect_iin_columns(data):
    """Detect IIN or IIN_BIN columns in query results"""
    if not data or len(data) == 0:
        return None
    
    # Check column names for IIN-like patterns
    first_row = data[0]
    for column_name in first_row.keys():
        column_upper = column_name.upper()
        if 'IIN' in column_upper or 'IIN_BIN' in column_upper:
            return column_name
    
    return None

def extract_iin_values(data, iin_column):
    """Extract unique IIN values from query results"""
    if not data or not iin_column:
        return []
    
    iin_values = set()
    for row in data:
        iin_value = row.get(iin_column)
        if iin_value and str(iin_value).strip():
            iin_values.add(str(iin_value).strip())
    
    return list(iin_values)

# SC Local Tables Management Functions
def insert_control_group(theory_id, iin_values, date_start, date_end, additional_fields=None):
    """Insert control group users into SC_local_control table"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Prepare additional fields (tab1-tab5)
        tab_fields = additional_fields or {}
        tab1 = tab_fields.get('tab1', None)
        tab2 = tab_fields.get('tab2', None)
        tab3 = tab_fields.get('tab3', None)
        tab4 = tab_fields.get('tab4', None)
        tab5 = tab_fields.get('tab5', None)
        
        insert_sql = """
        INSERT INTO SC_local_control 
        (IIN, THEORY_ID, date_start, date_end, insert_datetime, tab1, tab2, tab3, tab4, tab5)
        VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), TO_DATE(:4, 'YYYY-MM-DD'), SYSDATE, :5, :6, :7, :8, :9)
        """
        
        inserted_count = 0
        for iin in iin_values:
            try:
                cursor.execute(insert_sql, (
                    str(iin).strip(),
                    theory_id,
                    date_start,
                    date_end,
                    tab1, tab2, tab3, tab4, tab5
                ))
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting control IIN {iin}: {e}")
                continue
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "inserted_count": inserted_count,
            "message": f"Inserted {inserted_count} users into control group"
        }
        
    except Exception as e:
        print(f"Error inserting control group: {e}")
        return {
            "success": False,
            "message": f"Failed to insert control group: {str(e)}"
        }

def insert_into_spss_theory_users(theory_id, iin_values, date_start, date_end, additional_fields=None):
    """Insert target group users into SC_theory_users table in SPSS database"""
    try:
        connection = get_connection_SPSS()
        cursor = connection.cursor()
        
        # Prepare additional fields (tab1-tab5)
        tab_fields = additional_fields or {}
        tab1 = tab_fields.get('tab1', None)
        tab2 = tab_fields.get('tab2', None)
        tab3 = tab_fields.get('tab3', None)
        tab4 = tab_fields.get('tab4', None)
        tab5 = tab_fields.get('tab5', None)
        
        insert_sql = """
        INSERT INTO SC_theory_users 
        (IIN, THEORY_ID, date_start, date_end, insert_datetime, tab1, tab2, tab3, tab4, tab5)
        VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), TO_DATE(:4, 'YYYY-MM-DD'), SYSDATE, :5, :6, :7, :8, :9)
        """
        
        inserted_count = 0
        for iin in iin_values:
            try:
                cursor.execute(insert_sql, (
                    str(iin).strip(),
                    theory_id,
                    date_start,
                    date_end,
                    tab1, tab2, tab3, tab4, tab5
                ))
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting target IIN {iin} into SPSS: {e}")
                continue
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "inserted_count": inserted_count,
            "message": f"Inserted {inserted_count} users into SPSS SC_theory_users table"
        }
        
    except Exception as e:
        print(f"Error inserting into SPSS SC_theory_users: {e}")
        return {
            "success": False,
            "message": f"Failed to insert into SPSS SC_theory_users: {str(e)}"
        }

def insert_target_groups(theory_id, iin_values, date_start, date_end, additional_fields=None):
    """Insert target group users into SC_local_target table and duplicate to SPSS SC_theory_users table"""
    results = {
        "dssb_app": None,
        "spss": None,
        "overall_success": False,
        "total_inserted": 0,
        "messages": []
    }
    
    # Prepare additional fields (tab1-tab5)
    tab_fields = additional_fields or {}
    tab1 = tab_fields.get('tab1', None)
    tab2 = tab_fields.get('tab2', None)
    tab3 = tab_fields.get('tab3', None)
    tab4 = tab_fields.get('tab4', None)
    tab5 = tab_fields.get('tab5', None)
    
    # 1. Insert into DSSB_APP SC_local_target table
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        insert_sql = """
        INSERT INTO SC_local_target 
        (IIN, THEORY_ID, date_start, date_end, insert_datetime, tab1, tab2, tab3, tab4, tab5)
        VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), TO_DATE(:4, 'YYYY-MM-DD'), SYSDATE, :5, :6, :7, :8, :9)
        """
        
        dssb_inserted_count = 0
        for iin in iin_values:
            try:
                cursor.execute(insert_sql, (
                    str(iin).strip(),
                    theory_id,
                    date_start,
                    date_end,
                    tab1, tab2, tab3, tab4, tab5
                ))
                dssb_inserted_count += 1
            except Exception as e:
                print(f"Error inserting target IIN {iin} into DSSB_APP: {e}")
                continue
        
        connection.commit()
        cursor.close()
        connection.close()
        
        results["dssb_app"] = {
            "success": True,
            "inserted_count": dssb_inserted_count,
            "message": f"Inserted {dssb_inserted_count} users into DSSB_APP SC_local_target"
        }
        results["total_inserted"] += dssb_inserted_count
        results["messages"].append(f"DSSB_APP: {dssb_inserted_count} users inserted into SC_local_target")
        
    except Exception as e:
        print(f"Error inserting target groups into DSSB_APP: {e}")
        results["dssb_app"] = {
            "success": False,
            "message": f"Failed to insert into DSSB_APP SC_local_target: {str(e)}"
        }
        results["messages"].append(f"DSSB_APP Error: {str(e)}")
    
    # 2. Duplicate insert into SPSS SC_theory_users table
    try:
        spss_result = insert_into_spss_theory_users(theory_id, iin_values, date_start, date_end, additional_fields)
        results["spss"] = spss_result
        
        if spss_result["success"]:
            results["total_inserted"] += spss_result["inserted_count"]
            results["messages"].append(f"SPSS: {spss_result['inserted_count']} users inserted into SC_theory_users")
        else:
            results["messages"].append(f"SPSS Error: {spss_result['message']}")
            
    except Exception as e:
        print(f"Error duplicating to SPSS: {e}")
        results["spss"] = {
            "success": False,
            "message": f"Failed to duplicate to SPSS: {str(e)}"
        }
        results["messages"].append(f"SPSS Error: {str(e)}")
    
    # Determine overall success
    dssb_success = results["dssb_app"] and results["dssb_app"]["success"]
    spss_success = results["spss"] and results["spss"]["success"]
    
    # Overall success if at least DSSB_APP succeeded (SPSS is bonus)
    results["overall_success"] = dssb_success
    
    # Create consolidated response
    if dssb_success and spss_success:
        message = f"Successfully inserted into both databases: {results['messages'][0]}, {results['messages'][1]}"
    elif dssb_success:
        message = f"Primary insertion successful: {results['messages'][0]}. SPSS duplication failed: {results['messages'][1] if len(results['messages']) > 1 else 'Unknown error'}"
    else:
        message = f"Primary insertion failed: {results['messages'][0] if results['messages'] else 'Unknown error'}"
    
    return {
        "success": results["overall_success"],
        "inserted_count": results["dssb_app"]["inserted_count"] if results["dssb_app"] and results["dssb_app"]["success"] else 0,
        "message": message,
        "detailed_results": results
    }

def get_sc_local_data(table_name, theory_id=None):
    """Get data from SC_local_control or SC_local_target tables"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Validate table name for security
        if table_name not in ['SC_local_control', 'SC_local_target']:
            raise ValueError("Invalid table name")
        
        base_query = f"""
        SELECT IIN, THEORY_ID, 
               TO_CHAR(date_start, 'YYYY-MM-DD') as date_start,
               TO_CHAR(date_end, 'YYYY-MM-DD') as date_end,
               TO_CHAR(insert_datetime, 'YYYY-MM-DD HH24:MI:SS') as insert_datetime,
               tab1, tab2, tab3, tab4, tab5
        FROM {table_name}
        """
        
        if theory_id:
            query = base_query + " WHERE THEORY_ID = :1 ORDER BY insert_datetime DESC"
            cursor.execute(query, (theory_id,))
        else:
            query = base_query + " ORDER BY insert_datetime DESC"
            cursor.execute(query)
        
        columns = [desc[0].lower() for desc in cursor.description]
        
        data = []
        for row in cursor.fetchall():
            data.append(dict(zip(columns, row)))
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "data": data,
            "message": f"Retrieved {len(data)} records from {table_name}"
        }
        
    except Exception as e:
        print(f"Error getting SC local data: {e}")
        return {
            "success": False,
            "data": [],
            "message": f"Failed to get data: {str(e)}"
        }

# Daily Automated Process Functions
def get_active_campaigns_for_daily_process():
    """Get active campaigns that should receive new users from daily process"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Get campaigns that are currently active
        query = """
        SELECT 
            theory_id,
            theory_name,
            TO_CHAR(theory_start_date, 'YYYY-MM-DD') as theory_start_date,
            TO_CHAR(theory_end_date, 'YYYY-MM-DD') as theory_end_date,
            user_count
        FROM SoftCollection_theories
        WHERE SYSDATE BETWEEN theory_start_date AND theory_end_date
        ORDER BY theory_id
        """
        
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        
        campaigns = []
        for row in cursor.fetchall():
            campaigns.append(dict(zip(columns, row)))
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "campaigns": campaigns,
            "count": len(campaigns),
            "message": f"Found {len(campaigns)} active campaigns"
        }
        
    except Exception as e:
        print(f"Error getting active campaigns for daily process: {e}")
        return {
            "success": False,
            "campaigns": [],
            "count": 0,
            "message": f"Failed to get active campaigns: {str(e)}"
        }

def get_spss_count_day_5_users():
    """Get users from SPSS_USER_DRACRM.SC_1_120 where COUNT_DAY > 5 and COUNT_DAY < 31"""
    try:
        connection = get_connection_SPSS()
        cursor = connection.cursor()
        
        # Query for users with COUNT_DAY > 5 and COUNT_DAY < 31
        query = """
        SELECT IIN
        FROM SPSS_USER_DRACRM.SC_1_120
        WHERE COUNT_DAY > 5 and COUNT_DAY < 31 and COLLECTOR_COMPANY = 'ZRSLN_KOMP.08' or COLLECTOR_COMPANY = '-1' or COLLECTOR_COMPANY = 'ZRSLN_KOMP.101' or COLLECTOR_COMPANY = 'ZRSLN_KOMP.99'
        ORDER BY IIN
        """
        
        cursor.execute(query)
        
        # Extract IIN values
        iin_values = []
        for row in cursor.fetchall():
            iin = row[0]
            if iin and str(iin).strip():
                iin_values.append(str(iin).strip())
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "iin_values": list(set(iin_values)),  # Remove duplicates
            "count": len(set(iin_values)),
            "message": f"Found {len(set(iin_values))} unique users with COUNT_DAY > 5 and COUNT_DAY < 31"
        }
        
    except Exception as e:
        print(f"Error getting SPSS COUNT_DAY COUNT_DAY > 5 and COUNT_DAY < 31 users: {e}")
        return {
            "success": False,
            "iin_values": [],
            "count": 0,
            "message": f"Failed to get SPSS users: {str(e)}"
        }

def get_existing_campaign_groups(base_campaign_id):
    """Get existing groups for a campaign and their tab field values"""
    try:
        connection = get_connection_DSSB_APP()
        cursor = connection.cursor()
        
        # Find existing groups in both control and target tables
        existing_groups = {}
        
        # Check control table
        control_query = r"""
        SELECT DISTINCT THEORY_ID, tab1, tab2, tab3, tab4, tab5
        FROM SC_local_control 
        WHERE THEORY_ID LIKE :1
        """
        cursor.execute(control_query, (f"{base_campaign_id}%",))
        
        for row in cursor.fetchall():
            theory_id, tab1, tab2, tab3, tab4, tab5 = row
            existing_groups[theory_id] = {
                "table": "SC_local_control",
                "group_type": "control",
                "tab_values": {
                    "tab1": tab1,
                    "tab2": tab2, 
                    "tab3": tab3,
                    "tab4": tab4,
                    "tab5": tab5
                }
            }
        
        # Check target table  
        target_query = r"""
        SELECT DISTINCT THEORY_ID, tab1, tab2, tab3, tab4, tab5
        FROM SC_local_target 
        WHERE THEORY_ID LIKE :1
        """
        cursor.execute(target_query, (f"{base_campaign_id}%",))
        
        for row in cursor.fetchall():
            theory_id, tab1, tab2, tab3, tab4, tab5 = row
            existing_groups[theory_id] = {
                "table": "SC_local_target", 
                "group_type": "target",
                "tab_values": {
                    "tab1": tab1,
                    "tab2": tab2,
                    "tab3": tab3, 
                    "tab4": tab4,
                    "tab5": tab5
                }
            }
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "groups": existing_groups,
            "count": len(existing_groups),
            "message": f"Found {len(existing_groups)} existing groups for campaign {base_campaign_id}"
        }
        
    except Exception as e:
        print(f"Error getting existing campaign groups: {e}")
        return {
            "success": False,
            "groups": {},
            "count": 0,
            "message": f"Failed to get existing groups: {str(e)}"
        }

def distribute_users_to_campaigns(iin_values, campaigns):
    """Distribute users equally among active campaigns into their EXISTING groups"""
    try:
        if not iin_values or not campaigns:
            return {
                "success": False,
                "distributions": [],
                "message": "No users or campaigns available for distribution"
            }
        
        total_users = len(iin_values)
        total_campaigns = len(campaigns)
        
        # Calculate equal distribution among campaigns
        base_count = total_users // total_campaigns
        remainder = total_users % total_campaigns
        
        distributions = []
        start_idx = 0
        
        for i, campaign in enumerate(campaigns):
            # Last campaigns get the remainder
            users_for_campaign = base_count + (1 if i >= (total_campaigns - remainder) else 0)
            end_idx = start_idx + users_for_campaign
            
            campaign_users = iin_values[start_idx:end_idx]
            
            if campaign_users:
                # Extract base campaign ID (e.g., "SC00000001" from "SC00000001.1")
                base_campaign_id = campaign["theory_id"]
                if "." in base_campaign_id:
                    base_campaign_id = base_campaign_id.split(".")[0]
                
                # Get existing groups for this campaign
                existing_groups_result = get_existing_campaign_groups(base_campaign_id)
                
                if not existing_groups_result["success"] or not existing_groups_result["groups"]:
                    print(f"No existing groups found for campaign {base_campaign_id}, skipping...")
                    start_idx = end_idx
                    continue
                
                existing_groups = existing_groups_result["groups"]
                group_count = len(existing_groups)
                
                # Distribute users among existing groups
                users_per_group = len(campaign_users) // group_count
                group_remainder = len(campaign_users) % group_count
                
                group_distributions = {}
                group_start_idx = 0
                
                for group_idx, (theory_id, group_info) in enumerate(existing_groups.items()):
                    # Last groups get the remainder
                    users_for_group = users_per_group + (1 if group_idx >= (group_count - group_remainder) else 0)
                    group_end_idx = group_start_idx + users_for_group
                    
                    group_users = campaign_users[group_start_idx:group_end_idx]
                    if group_users:
                        # Extract group letter from theory ID (e.g., "A" from ".1", "B" from ".2", etc.)
                        suffix = theory_id.split(".")[-1] if "." in theory_id else "1"
                        group_letter = chr(ord('A') + int(suffix) - 1)  # 1->A, 2->B, 3->C, etc.
                        
                        group_distributions[group_letter] = {
                            "users": group_users,
                            "theory_id": theory_id,
                            "group_type": group_info["group_type"],
                            "target_table": group_info["table"],
                            "tab_values": group_info["tab_values"]
                        }
                    
                    group_start_idx = group_end_idx
                
                distributions.append({
                    "campaign": campaign,
                    "base_campaign_id": base_campaign_id,
                    "total_users": len(campaign_users),
                    "existing_groups_count": group_count,
                    "groups": group_distributions
                })
            
            start_idx = end_idx
        
        return {
            "success": True,
            "distributions": distributions,
            "total_users_distributed": sum(d["total_users"] for d in distributions),
            "message": f"Distributed {total_users} users among {total_campaigns} campaigns into existing groups"
        }
        
    except Exception as e:
        print(f"Error distributing users to campaigns: {e}")
        return {
            "success": False,
            "distributions": [],
            "message": f"Failed to distribute users: {str(e)}"
        }

def insert_daily_distributed_users(distributions):
    """Insert distributed users into existing groups with their original tab values"""
    try:
        results = {
            "total_inserted": 0,
            "campaign_results": [],
            "success": True,
            "messages": []
        }
        
        for distribution in distributions:
            campaign = distribution["campaign"]
            base_campaign_id = distribution["base_campaign_id"]
            date_start = campaign["theory_start_date"]
            date_end = campaign["theory_end_date"]
            groups = distribution["groups"]
            
            campaign_result = {
                "base_campaign_id": base_campaign_id,
                "campaign_name": campaign["theory_name"],
                "group_results": {},
                "total_inserted": 0,
                "success": True
            }
            
            # Process each group
            for group_letter, group_data in groups.items():
                if not group_data.get("users"):
                    continue
                
                group_users = group_data["users"]
                group_theory_id = group_data["theory_id"]
                group_type = group_data["group_type"]
                target_table = group_data["target_table"]
                existing_tab_values = group_data["tab_values"]
                
                try:
                    if group_type == "control":
                        # Insert into control table using existing tab values
                        result = insert_control_group(
                            theory_id=group_theory_id,
                            iin_values=group_users,
                            date_start=date_start,
                            date_end=date_end,
                            additional_fields=existing_tab_values
                        )
                        
                        campaign_result["group_results"][group_letter] = {
                            "theory_id": group_theory_id,
                            "target_table": "SC_local_control",
                            "users_count": len(group_users),
                            "inserted_count": result.get("inserted_count", 0),
                            "success": result.get("success", False),
                            "message": result.get("message", ""),
                            "existing_tab_values": existing_tab_values
                        }
                        
                    else:  # target group
                        # Insert into target table + SPSS using existing tab values
                        result = insert_target_groups(
                            theory_id=group_theory_id,
                            iin_values=group_users,
                            date_start=date_start,
                            date_end=date_end,
                            additional_fields=existing_tab_values
                        )
                        
                        campaign_result["group_results"][group_letter] = {
                            "theory_id": group_theory_id,
                            "target_table": "SC_local_target + SPSS",
                            "users_count": len(group_users),
                            "inserted_count": result.get("inserted_count", 0),
                            "success": result.get("success", False),
                            "message": result.get("message", ""),
                            "detailed_results": result.get("detailed_results", {}),
                            "existing_tab_values": existing_tab_values
                        }
                    
                    if result.get("success", False):
                        campaign_result["total_inserted"] += result.get("inserted_count", 0)
                    else:
                        campaign_result["success"] = False
                        
                except Exception as e:
                    print(f"Error inserting group {group_letter} for campaign {base_campaign_id}: {e}")
                    campaign_result["group_results"][group_letter] = {
                        "theory_id": group_theory_id,
                        "target_table": target_table,
                        "users_count": len(group_users),
                        "inserted_count": 0,
                        "success": False,
                        "message": f"Error: {str(e)}",
                        "existing_tab_values": existing_tab_values
                    }
                    campaign_result["success"] = False
            
            results["campaign_results"].append(campaign_result)
            results["total_inserted"] += campaign_result["total_inserted"]
            
            if not campaign_result["success"]:
                results["success"] = False
            
            # Create summary message for this campaign
            group_summaries = []
            for group_letter, group_result in campaign_result["group_results"].items():
                theory_id = group_result["theory_id"]
                count = group_result["inserted_count"]
                target = group_result["target_table"]
                tab1_value = group_result["existing_tab_values"].get("tab1", "NULL")
                group_summaries.append(f"Group {group_letter} ({theory_id}): {count} users → {target} [tab1: {tab1_value}]")
            
            campaign_message = f"Campaign {base_campaign_id} ({campaign['theory_name']}): {', '.join(group_summaries)}"
            results["messages"].append(campaign_message)
        
        return results
        
    except Exception as e:
        print(f"Error inserting daily distributed users: {e}")
        return {
            "total_inserted": 0,
            "campaign_results": [],
            "success": False,
            "messages": [f"Error: {str(e)}"]
        }

def process_daily_user_distribution():
    """Main function for daily automated user distribution process"""
    try:
        # Initialize result structure
        process_result = {
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "process_stage": "initialization",
            "campaigns_found": 0,
            "users_found": 0,
            "users_distributed": 0,
            "detailed_results": {},
            "error_message": None,
            "skip_reason": None
        }
        
        print(f"[{process_result['timestamp']}] Starting daily user distribution process...")
        
        # Step 1: Get active campaigns
        process_result["process_stage"] = "getting_active_campaigns"
        campaigns_result = get_active_campaigns_for_daily_process()
        
        if not campaigns_result["success"]:
            process_result["error_message"] = f"Failed to get active campaigns: {campaigns_result['message']}"
            return process_result
        
        if campaigns_result["count"] == 0:
            process_result["skip_reason"] = "no_active_campaigns"
            process_result["success"] = True  # Not an error, just nothing to do
            return process_result
        
        process_result["campaigns_found"] = campaigns_result["count"]
        print(f"Found {campaigns_result['count']} active campaigns")
        
        # Step 2: Get users with COUNT_DAY COUNT_DAY > 5 and COUNT_DAY < 31 from SPSS
        process_result["process_stage"] = "getting_spss_users"
        spss_users_result = get_spss_count_day_5_users()
        
        if not spss_users_result["success"]:
            process_result["error_message"] = f"Failed to get SPSS users: {spss_users_result['message']}"
            return process_result
        
        if spss_users_result["count"] == 0:
            process_result["skip_reason"] = "no_count_day_5_users"
            process_result["success"] = True  # Not an error, just nothing to do
            return process_result
        
        process_result["users_found"] = spss_users_result["count"]
        print(f"Found {spss_users_result['count']} users with COUNT_DAY > 5 and COUNT_DAY < 31")
        
        # Step 3: Distribute users among campaigns
        process_result["process_stage"] = "distributing_users"
        distribution_result = distribute_users_to_campaigns(
            spss_users_result["iin_values"],
            campaigns_result["campaigns"]
        )
        
        if not distribution_result["success"]:
            process_result["error_message"] = f"Failed to distribute users: {distribution_result['message']}"
            return process_result
        
        print(f"Distribution plan created for {distribution_result['total_users_distributed']} users")
        
        # Step 4: Insert distributed users into databases
        process_result["process_stage"] = "inserting_users"
        insertion_result = insert_daily_distributed_users(distribution_result["distributions"])
        
        process_result["users_distributed"] = insertion_result["total_inserted"]
        process_result["detailed_results"] = {
            "campaigns": campaigns_result["campaigns"],
            "distribution_plan": distribution_result["distributions"],
            "insertion_results": insertion_result["campaign_results"]
        }
        
        if insertion_result["success"]:
            process_result["success"] = True
            process_result["process_stage"] = "completed"
            print(f"Successfully distributed {insertion_result['total_inserted']} users")
        else:
            process_result["error_message"] = f"Insertion failed: {'; '.join(insertion_result['messages'])}"
            print(f"Insertion partially failed: {insertion_result['total_inserted']} users inserted")
        
        return process_result
        
    except Exception as e:
        print(f"Error in daily user distribution process: {e}")
        process_result["error_message"] = f"Unexpected error: {str(e)}"
        return process_result 