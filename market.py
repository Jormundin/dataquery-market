import streamlit as st
import cx_Oracle
import pandas as pd
from llama_cpp import Llama
import re
import numpy as np
import warnings
from io import BytesIO
from sklearn.model_selection import StratifiedGroupKFold
from scipy.stats import ks_2samp
from sklearn.model_selection import train_test_split
import random
from datetime import datetime
from sqlalchemy import text
from Jira import jira_main
from itertools import islice
import logging
from SQL_helper import sql_main
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
import getpass
from campaign_responsible import main_checker
warnings.filterwarnings('ignore')
import plotly.express as px
from typing import Optional, Dict, Tuple
from functools import lru_cache
import json
import requests
import plotly.graph_objects as go
import os
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_community.llms import Ollama
import webbrowser
from streamlit_extras.colored_header import colored_header
from streamlit_extras.add_vertical_space import add_vertical_space
from streamlit_extras.card import card
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.stateful_button import button
from streamlit_extras.tags import tagger_component
import io
import pyarrow as pa
import pyarrow.parquet as pq

def run_main_app():

    now = datetime.now()
   
    # Database connection function
    def get_connection():
        dsn = cx_Oracle.makedsn('', '', sid='')
        return cx_Oracle.connect(user='', password='', dsn=dsn)

    def get_connection_DSSB_OCDS():
        dsn = cx_Oracle.makedsn('', '', sid='')
        return cx_Oracle.connect(user='', password='', dsn=dsn)

    def get_connection_SPSS_OCDS():
        dsn = cx_Oracle.makedsn('', '', sid='')
        return cx_Oracle.connect(user='', password='', dsn=dsn)

    def get_connection_ED_OCDS():
        dsn = cx_Oracle.makedsn('', '', sid='')
        return cx_Oracle.connect(user='', password='', dsn=dsn)

    def clear_extra_iin():
        with get_connection_SPSS_OCDS() as conn:
            today = datetime.today()
            today_str = today.strftime("%d.%m.%Y")
            sql_query = f"""
                select f.p_sid, 1 as spssfl
                from spss_ocds.fd_rb2_campaigns_users f
                where f.upload_date = to_date('{today_str}','dd.mm.yyyy')
                group by f.p_sid
                having count(*)>1
                """
            constraint_df1 = pd.read_sql(sql_query, conn)
           
            st.session_state.df = st.session_state.df.merge(constraint_df1, on='P_SID', how='left')

    def fetch_campaign_data_by_id(conn, table_name):
        # Assuming 'campaign_id' is the column name in your table that holds the campaign ID.

        query = f"select max(CAMPAIGNCODE) as CAMPAIGNCODE from {table_name} where length(CAMPAIGNCODE) = 10 and CAMPAIGNCODE like 'C0000%'"

        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
       
        if result:
            # Assuming the result is a tuple in the order of the SQL table columns
            # Convert this to a dictionary where keys are the column names for easier access
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, result))
        else:
            return None

    def fetch_campaign_data_by_id_main(conn, CAMPAIGNCODE, table_name):

        query = f"SELECT * FROM {table_name} WHERE CAMPAIGNCODE = :CAMPAIGNCODE"
   
        cursor = conn.cursor()
        cursor.execute(query, {'CAMPAIGNCODE': CAMPAIGNCODE})
        result = cursor.fetchone()
       
        if result:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, result))
        else:
            return None

    def fetch_campaign_data_by_XLS(conn):
        # Assuming 'campaign_id' is the column name in your table that holds the campaign ID.

        query = f"select max(XLS_OW_ID) as XLS_OW_ID from dssb_ocds.rb3_tr_campaign_dict where length(XLS_OW_ID) = 8 and XLS_OW_ID like 'KKB_%'"

        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
       
        if result:
            # Assuming the result is a tuple in the order of the SQL table columns
            # Convert this to a dictionary where keys are the column names for easier access
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, result))
        else:
            return None

    def send_email(email_recipient, email_subject, email_message, attachment_location = ''):
   
            email_sender = 'cdsrb@halykbank.kz'
   
            msg = MIMEMultipart()
            msg['From'] = email_sender
            msg['To'] = ",".join(email_recipient)
            msg['Subject'] = email_subject
   
            msg.attach(MIMEText(email_message, 'plain'))
            if attachment_location != '':  
                filename = os.path.basename(attachment_location)
                attachment = open(attachment_location, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)      
                part.add_header('Content-Disposition',"attachment; filename= %s" % filename)
                msg.attach(part)
   
            try:
                server = smtplib.SMTP('mail.halykbank.nb', 587)
                server.ehlo()
                server.starttls()
                server.login('cdsrb', 'oREgNwtWr9B5F4dsGjjZ')
                text = msg.as_string()
                server.sendmail(email_sender, email_recipient, text)
                print("accepted")
                print('email sent')
                server.quit()
            except:
                print("SMPT server connection error")
            return True
   
    # Query database and return DataFrame
    def query_database(selected_columns, additional_filters, user_limit):
        where_clauses = []
        for col, filter_details in additional_filters.items():
            if filter_details[0] == 'BETWEEN':
                where_clauses.append(f"{col} BETWEEN {filter_details[1]} AND {filter_details[2]}")
            elif filter_details[0] == 'IN':
                formatted_values = ', '.join([f"'{val}'" for val in filter_details[1]])
                where_clauses.append(f"{col} IN ({formatted_values})")
               
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
       
        query = f"SELECT {', '.join(selected_columns)} FROM dssb_app.rb_feature_store WHERE {where_clause} and SNAPSHOT_DATE = (select MAX(SNAPSHOT_DATE) from dssb_app.rb_feature_store) OFFSET 0 ROWS FETCH NEXT {user_limit} ROWS ONLY"

       
        # Connect to database and execute query
        with get_connection() as conn:
            df = pd.read_sql(query, conn)
   
        return df




    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                return None
   
    def calculate_age(row):
        today = datetime.today()
       
        if row is None or pd.isnull(row['AGE']):
            birth_date = None
            if row is not None and row['BIRTH_DATE'] is not None and not pd.isnull(row['BIRTH_DATE']):
                birth_date = parse_date(row['BIRTH_DATE'])
            elif row is not None and row['IIN_BIN'] is not None and len(row['IIN_BIN']) >= 6:
                birth_date_str = row['IIN_BIN'][:6]
                try:
                    birth_date = datetime.strptime(birth_date_str, "%y%m%d")
                    # Adjust for century
                    if birth_date > today:
                        birth_date = birth_date.replace(year=birth_date.year - 100)
                except ValueError:
                    return None
           
            if birth_date is None:
                return None
           
            return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        else:
            return row['AGE']
   
    def determine_sex_by_iin(iin):
        if len(iin) > 6:
            if iin[6] in '135':
                return 'M'
            elif iin[6] in '246':
                return 'F'
        return None

    def load_sql_table(conn):
        query = 'select IIN_BIN, BIRTH_DATE, AGE, SEX_CODE  from dssb_dm.rb_clients'
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df2 = pd.DataFrame(rows, columns=columns)
        return df2


    def load_sql_table_stratification(conn):
        query = 'select IIN_BIN, AGE, SEX_CODE, MARITAL_STATUS, POSITION, EDUCATION from dssb_dm.rb_clients'
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df3 = pd.DataFrame(rows, columns=columns)
        return df3

    def get_iin_bin_for_filials(conn, selected_filials):
            cursor = conn.cursor()
   
            placeholders = ','.join(':' + str(i+1) for i in range(len(selected_filials)))
            query = f"""
                SELECT DISTINCT IIN_BIN AS IIN
                FROM dssb_dm.rb_clients
                WHERE FILIAL IN ({placeholders})
            """
   
            cursor.execute(query, selected_filials)
   
            results = cursor.fetchall()
   
            df_filials = pd.DataFrame(results, columns=['IIN'])
   
            return df_filials



    def filter_dataframe_extra(df2, min_age, max_age, sex_selection):
        df2['AGE'] = df2.apply(calculate_age, axis=1)
       
        if min_age is not None:
            df2 = df2[df2['AGE'] >= min_age]
        if max_age is not None:
            df2 = df2[df2['AGE'] <= max_age]
        if sex_selection:
            df2['SEX_CODE'] = df2.apply(lambda row: determine_sex_by_iin(row['IIN_BIN']) if pd.isnull(row['SEX_CODE']) else row['SEX_CODE'], axis=1)
            if 'M' in sex_selection and 'F' not in sex_selection:
                df2 = df2[df2['SEX_CODE'] == 'M']
            elif 'F' in sex_selection and 'M' not in sex_selection:
                df2 = df2[df2['SEX_CODE'] == 'F']
        return df2
   
    def filter_df1_based_on_df2(df1, df2):
        # Create a set of IIN_BIN values present in df2
        iin_bin_set = set(df2['IIN_BIN'])
   
        # Filter df1 to keep only rows where IIN is in the set of IIN_BIN values
        filtered_df1 = df1[df1['IIN'].isin(iin_bin_set)]
   
        return filtered_df1




   
   
    # Query database and return DataFrame
    def query_database_all(selected_columns, additional_filters):
        where_clauses = []
        for col, filter_details in additional_filters.items():
            if filter_details[0] == 'BETWEEN':
                where_clauses.append(f"{col} BETWEEN {filter_details[1]} AND {filter_details[2]}")
            elif filter_details[0] == 'IN':
                formatted_values = ', '.join([f"'{val}'" for val in filter_details[1]])
                where_clauses.append(f"{col} IN ({formatted_values})")
               
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
       
        query = f"SELECT {', '.join(selected_columns)} FROM dssb_app.rb_feature_store WHERE {where_clause} and SNAPSHOT_DATE = (select MAX(SNAPSHOT_DATE) from dssb_app.rb_feature_store)"

       
        # Connect to database and execute query
        with get_connection() as conn:
            df = pd.read_sql(query, conn)
   
        return df


    # Query database and return DataFrame
    def get_psid():      
        query = f"SELECT IIN, P_SID FROM dssb_app.rb_feature_store where SNAPSHOT_DATE = (select MAX(SNAPSHOT_DATE) from dssb_app.rb_feature_store)"
        # Connect to database and execute query
        with get_connection() as conn:
            df = pd.read_sql(query, conn)
   
        return df
   
   
   
   
    def fetch_IIN_to_keep(connection, IIN_list, status_columns):
        # Function to fetch IINs to keep, adjusted for your SQL logic
        status_checks = ' OR '.join([f"{col} = 0" for col in status_columns])
        formatted_IIN_list = ','.join(f"'{item}'" for item in IIN_list)
        sql_query = f"""
        SELECT IIN
        FROM dssb_app.rb_feature_store
        WHERE (IIN IN ({formatted_IIN_list})) AND ({status_checks})
        """
        cursor = connection.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        IINs_to_keep = [result[0] for result in results]
        return IINs_to_keep
   
    def fetch_IIN_to_keep_reverce(connection, IIN_list, status_columns):
        # Function to fetch IINs to keep, adjusted for your SQL logic
        status_checks = ' OR '.join([f"{col} = 1" for col in status_columns])
        formatted_IIN_list = ','.join(f"'{item}'" for item in IIN_list)
        sql_query = f"""
        SELECT IIN
        FROM dssb_app.rb_feature_store
        WHERE (IIN IN ({formatted_IIN_list})) AND ({status_checks})
        """
        cursor = connection.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        IINs_to_keep = [result[0] for result in results]
        return IINs_to_keep
   
   
    def add_columns_and_filter_nulls(df, conn, additional_table, join_column, additional_columns):
        # Construct a query to retrieve the additional columns
        # Ensure the query excludes rows where either of the additional columns is NULL
        additional_columns_str = ', '.join(additional_columns)
        query = f"""
        SELECT a.{join_column}, {additional_columns_str}
        FROM {additional_table} a
        JOIN dssb_app.rb_feature_store b ON a.{join_column} = b.{join_column}
        WHERE a.{additional_columns[0]} IS NOT NULL AND a.{additional_columns[1]} IS NOT NULL
        """
       
        # Execute the query
        additional_df = pd.read_sql(query, conn)
       
        # Merge the additional data with the original DataFrame
        final_df = pd.merge(df, additional_df, on=join_column, how='inner')
       
        return final_df
   
   
   
    # Assuming 'information_columns' and 'blacklist_columns' are provided as lists
    information_columns = ['SNAPSHOT_DATE','IIN','P_SID','PUBLIC_ID', 'IS_MAU']  # columns
   
    sum_columns = [
        'CARD_PURCHASE_FREQUENCY', 'CARD_PURCHASE_SUM', 'LOAN_CNT',    'SAFE_CNT',    'ACCOUNT_CNT', 'CREDIT_CARD_CNT', 'DEBIT_CARD_CNT', 'BEZNAL_TRANSACTION_CNT' ,'BEZNAL_AMOUNT', 'OBNAL_TRANSACTION_CNT']
   
    stop_list_columns = [
    'CORP_CARD'
    ,'CUR_CAMP_FIZ_IN'
    ]  # blacklist columns
   
    blacklist_columns = [
    'BL_BANKRUPT'
    ,'BL_DECEASED'
    ]
   
    ALL_STOP_LIST = [
    'EPG_ECG_FL'
    ]
   
    Extra_STOP_LIST = ['SALARY_FL']
   
   
    def fetch_IINs_to_exclude(conn, selected_stop_list_columns, user_id_col_name, user_ids, user_limit):
        """
        Fetch IINs that should be excluded based on stop list and blacklist columns.
        """
        # Construct SQL query to select IINs that match stop list or blacklist criteria
        status_checks = ' OR '.join([f"{col} = 1" for col in selected_stop_list_columns if col.strip()])
        if not status_checks:
            return []
   
        user_ids_str = ','.join(f"'{item}'" for item in user_ids)
   
        sql_query = f"""
        SELECT IIN
        FROM dssb_app.rb_feature_store
        WHERE {status_checks} AND {user_id_col_name} IN ({user_ids_str})
        OFFSET 0 ROWS FETCH NEXT {user_limit} ROWS ONLY
        """
   
   
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        cursor.close()
   
        # Extract IINs from query results
        excluded_IINs = [result[0] for result in results]
       
        return excluded_IINs

    #def fetch_data(table_name, conn):
        #"""Fetch user IDs from a specific table in the Oracle database."""
        #query = f"SELECT DISTINCT IIN FROM {table_name}"
        #return pd.read_sql(query, conn)

    tables = ['ACRM_DW.RB_BLACK_LIST@ACRM', 'dssb_de.dim_clients_black_list', 'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK', 'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK' ,  'dssb_app.not_recommend_credits']

    def fetch_data(table):

        if table == 'ACRM_DW.RB_BLACK_LIST@ACRM':
            parquet_file = 'Databases/ACRM_DW.RB_BLACK_LIST@ACRM.parquet'
        if table == 'dssb_de.dim_clients_black_list':
            parquet_file = 'Databases/dssb_de.dim_clients_black_list.parquet'
        if table == 'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK':
            parquet_file = 'Databases/SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK.parquet'
        if table == 'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK':
            parquet_file = 'Databases/SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK.parquet'
        if table == 'dssb_app.not_recommend_credits':
            parquet_file = 'Databases/dssb_app.not_recommend_credits.parquet'
        if table == 'DSSB_OCDS.mb11_global_control':
            parquet_file = 'Databases/DSSB_OCDS.mb11_global_control.parquet'
        if table == 'BL_No_worker':
            parquet_file = 'Databases/BL_No_worker.parquet'
        if table == 'dssb_app.abc_nbo_only':
            parquet_file = 'Databases/dssb_app.abc_nbo_only.parquet'
        if table == 'dssb_app.abc_ptb_models':
            parquet_file = 'Databases/dssb_app.abc_ptb_models.parquet'
        if table == 'dssb_app.abc_nbo_and_market':
            parquet_file = 'Databases/dssb_app.abc_nbo_and_market.parquet'
        return pd.read_parquet(parquet_file)

    #def fetch_data_push(table_name, conn):
    #    """Fetch user IDs from a specific table in the Oracle database."""
    #    query = f"select distinct g.iin from DSSB_SE.UCS_HB_PUSH po left join dssb_se.gid_map2 g on po.owid = g.p_sid_ow group by g.iin having #min(po.is_push_off) = 1"
    #    return pd.read_sql(query, conn)

    def fetch_data_push():
        parquet_file = 'Databases/DSSB_SE.UCS_HB_PUSH.parquet'
        return pd.read_parquet(parquet_file)

    #def fetch_data_extra_filters(extra_stream, conn):
    #    """Fetch user IDs from a specific table in the Oracle database."""
    #    query = f"select distinct g.iin from DSSB_DE.UCS_PUSH_OFF po left join dssb_se.gid_map2 g on po.owid = g.p_sid_ow left join DSSB_DE.DICT_UCS_EVENTS e on po.eventid = po.eventid where e.eventdescription like '%{extra_stream}%'"
    #    return pd.read_sql(query, conn)


    def fetch_data_extra_filters(selected_extra_streams):
        df = pd.read_parquet('Databases/DSSB_DE.UCS_PUSH_OFF.parquet')
       
        # Check what columns are available
        print(f"Available columns: {df.columns.tolist()}")
       
        if 'EVENTDESCRIPTION' in df.columns:
            filtered_df = df[df['EVENTDESCRIPTION'].isin(selected_extra_streams)]
        else:
            print("Warning: EVENTDESCRIPTION column not found. Using IIN column instead.")
            filtered_df = df[df['IIN'].isin(selected_extra_streams)]
       
        return filtered_df

    #def fetch_data_device(device, conn):
    #    """Fetch user IDs from a specific table in the Oracle database."""
    #    query = f"select distinct CLIENT_IIN IIN from dssb_dm.hb_sessions_fl h where h.SNAPSHOT_DATE> sysdate-2 and OPERATIONSYSTEM like '%{device}%'"
    #    return pd.read_sql(query, conn)

    def fetch_data_device(selected_devices):
        df = pd.read_parquet('Databases/dssb_dm.hb_sessions_fl.parquet')
        filtered_df = df[df['OPERATIONSYSTEM'].isin(selected_devices)]
        #st.info(selected_devices)
        return filtered_df

    def fetch_data_stream_target(stream, conn):
        """Fetch user IDs from a specific table in the Oracle database."""
        query = f"select distinct iin from dssb_ocds.mb22_local_target where lower(stream) like '%{stream}%' and date_end > sysdate + 0.5"
        return pd.read_sql(query, conn)

    def fetch_data_stream_control(stream, conn):
        """Fetch user IDs from a specific table in the Oracle database."""
        query = f"select distinct iin from DSSB_OCDS.mb21_local_control where lower(stream) like '%{stream}%' and date_end > sysdate + 0.5"
        return pd.read_sql(query, conn)

    def fetch_data_stream_control_target(stream, conn):
        """Fetch user IDs from a specific table in the Oracle database."""
        query = f"select distinct iin from DSSB_OCDS.mb21_local_control where lower(stream) like '%{stream}%' and date_end > sysdate + 0.5 union select distinct iin from dssb_ocds.mb22_local_target where lower(stream) like '%{stream}%' and date_end > sysdate + 0.5"
        return pd.read_sql(query, conn)


    def fetch_data_stream_target_rb3(stream_rb3, conn):
        query = f"select distinct t.iin from dssb_ocds.mb01_camp_dict d inner join dssb_ocds.mb22_local_target t on t.campaigncode = d.campaigncode where lower(d.sub_stream) like '%{stream_rb3}%' and date_end > sysdate + 0.5"
        return pd.read_sql(query, conn)

    def fetch_data_stream_control_rb3(strea_rb3, conn):
        query = f"select distinct t.iin from dssb_ocds.mb01_camp_dict d inner join dssb_ocds. mb22_local_control t on t.campaigncode = d.campaigncode where lower(d.sub_stream) like '%{stream_rb3}%' and date_end > sysdate + 0.5"
        return pd.read_sql(query, conn)

    def fetch_data_stream_control_target_rb3(stream_rb3, conn):
        query = f"""select distinct t.iin
from dssb_ocds.mb01_camp_dict d
inner join dssb_ocds.mb22_local_target t on t.campaigncode = d.campaigncode
where lower(d.sub_stream) like '%{stream_rb3}%' and date_end > sysdate + 0.5
union
select distinct t.iin
from dssb_ocds.mb01_camp_dict d
inner join dssb_ocds. mb22_local_control t on t.campaigncode = d.campaigncode
where lower(d.sub_stream) like '%{stream_rb3}%' and date_end > sysdate + 0.5"""
        return pd.read_sql(query, conn)



    def fetch_data_prev_campaign(prev_campaign, conn):
        query = f"select distinct CUSTOMER_IIN from dds.mc_campaign_fl where CAMPAIGNCODE = '{prev_campaign}'"
        return pd.read_sql(query, conn)

    def fetch_data_base_more_than_one(conn, date):
        """Fetch user IDs from a specific table in the Oracle database."""
        today_str = date.strftime("%d.%m.%Y")
        query = f"select f.IIN from spss_ocds.fd_rb2_campaigns_users f where f.upload_date = to_date('{today_str}','dd.mm.yyyy') group by f.IIN having count(*)>1"
        result_df = pd.read_sql(query, conn)
        
        # Ensure IIN column is properly formatted as string to maintain consistency
        if not result_df.empty and 'IIN' in result_df.columns:
            result_df['IIN'] = result_df['IIN'].astype(str)
            
        return result_df

    def fetch_phones(conn):
        query = f"select pr.iin, pr.phone from dssb_app.gidmap_contacts pr where pr.trusted = 1 and pr.actual = 1"
        return pd.read_sql(query, conn)


    def fetch_data_from_sql_query(conn, sql_script):
        query = sql_script
        return pd.read_sql(query, conn)

#    def filter_df_by_iin(df, conn):
#        cursor = conn.cursor()
#
#        cursor.execute("select iin from dssb_dev.dssb_push_analytics where hb_1m = 1")
#        iin_values = cursor.fetchall()
#      
#        # Convert fetched values to a set for faster lookup
#        iin_set = set(iin[0] for iin in iin_values)
#      
#        # Filter the DataFrame
#        filtered_df = df[df['IIN'].isin(iin_set)]
#      
#        return filtered_df

    def filter_df_by_iin(df):
   
        iin_values = pd.read_parquet('Databases/dssb_dev.dssb_push_analytics.parquet')
        iin_values = list(iin_values.itertuples(index = False, name = None))
       
        iin_set = set(iin[0] for iin in iin_values)
       
        # Filter the DataFrame
        filtered_df = df[df['IIN'].isin(iin_set)]
       
        return filtered_df
   
   
   
   
    def filter_dataframe(df, conn, selected_stop_list_columns,user_id_col_name, user_ids):
        """
        Filter the DataFrame by excluding rows based on IINs that meet
        stop list or blacklist criteria in the SQL database.
        """
        excluded_IINs = fetch_IINs_to_exclude(conn, selected_stop_list_columns, user_id_col_name, user_ids, user_limit)
   
        # Proceed with filtering if there are IINs to exclude
        if excluded_IINs:
            df = df[~df['IIN'].isin(excluded_IINs)]
           
       
        return df


    def filter_dataframe_two(df, conn, selected_stop_list_columns,user_id_col_name, user_ids):

        cursor.execute("""
        INSERT INTO bn_test (IIN)
        VALUES (:user_ids)
        """)

        cursor.execute("""
        select IIN from bn_test
        where IIN not in (select distinct iin from ACRM_DW.RB_BLACK_LIST@ACRM)
        """)
       
           
       
        return df
   
   
    def stratify_dataframe_and_ks_test(df, stratify_cols, train_size):
        st.write("Изначальный размер DF:", df.shape)
       
        df = df.dropna(subset=stratify_cols)
        st.write("Размер DF после очистки от NULL:", df.shape)
   
        grouped = df.groupby(stratify_cols).filter(lambda x: len(x) > 1)
        st.write("Размер DF после очиски единичных классов:", grouped.shape)
   
        if grouped.empty:
            st.write("No valid combinations left after filtering. Consider adjusting stratification columns.")
            return None, None, "No valid combinations left after filtering."
   
        rand_int = random.randint(1, 101)
   
        try:
            df1, df2 = train_test_split(grouped, train_size=train_size, random_state=rand_int, stratify=grouped[stratify_cols])
            st.write("Размеры DF1 и DF2:", df1.shape, df2.shape)
        except ValueError as e:
            st.write("Error during train_test_split:", e)
            return None, None, f"Error during stratification: {e}"
   
        ks_test_results = []
        for col in grouped.columns:
            if grouped[col].dtype.kind in 'bifc' and col not in stratify_cols:
                if grouped[col].isnull().any():
                    st.write(f"Skipping column {col} due to NaN values.")
                    continue
                stat, p_value = ks_2samp(df1[col].dropna(), df2[col].dropna())
                ks_test_results.append({'Column': col, 'KS statistic': stat, 'p-value': p_value})
   
        return df1, df2, ks_test_results
       
   
   
    def increment_string(s):
        prefix = s[0]  # Get the first character
        number_str = s[1:]  # Get the numeric part as a string
        number = int(number_str)  # Convert the numeric part to an integer
        incremented_number = number + 1  # Increment the number
        incremented_number_str = str(incremented_number).zfill(len(number_str))  # Convert back to string, preserving leading zeros
        return f"{prefix}{incremented_number_str}"

    def increment_string_XLS(s):
        prefix = s[0]  # Get the first character
        prefix1 = s[1]  # Get the first character
        prefix2 = s[2]  # Get the first character
        prefix3 = s[3]  # Get the first character
        number_str = s[4:]  # Get the numeric part as a string
        number = int(number_str)  # Convert the numeric part to an integer
        incremented_number = number + 1  # Increment the number
        incremented_number_str = str(incremented_number).zfill(len(number_str))  # Convert back to string, preserving leading zeros
        return f"{prefix}{prefix1}{prefix2}{prefix3}{incremented_number_str}"



    def add_column_sum(df, selected_columns):
        if not all(column in df.columns for column in selected_columns):
            raise ValueError('One or more selected columns not in a dataframe')

        df['Column_sum'] = df[selected_columns].sum(axis=1)
        return df
   
   
   
   
    def merge_summed_data_with_df(original_df, summed_df, user_id_col_name):
        # Merge the original DataFrame with the summed DataFrame on user_id
        #st.write('//////////////////////////////////////////////////////////////////')
        #st.write(original_df)
        #st.write(summed_df)
        #st.write(user_id_col_name)
        merged_df = pd.merge(original_df, summed_df, on=user_id_col_name, how='left')
        return merged_df
   
   
    # Add function to load products from parquet file
    def load_products_from_parquet(parquet_file_path):
        """Load product data from parquet file"""
        try:
            parquet_base_dir = os.getenv('PARQUET_OUTPUT_DIR', 'Databases')
            full_path = os.path.join(parquet_base_dir, parquet_file_path)
           
            if not os.path.exists(full_path):
                st.error(f"Product file not found at {full_path}")
                return pd.DataFrame()
           
            df = pd.read_parquet(full_path)

            if "iin" in df.columns and 'IIN' not in df.columns:
                df = df.rename(columns={'iin': 'IIN'})
                st.info('Renamed column iin to IIN for consistency')

            # Ensure IIN data consistency
            if 'IIN' in df.columns:
                df['IIN'] = df['IIN'].astype(str).str.strip()

           
            return df
        except Exception as e:
            st.error(f"Error loading product data: {e}")
            return pd.DataFrame()

    def get_available_products(products_df):
        """Extract unique products from the products dataframe"""
        if 'sku_level1' in products_df.columns:
            return sorted(products_df['sku_level1'].dropna().unique().tolist())
        else:
            st.error("Product column 'sku_level1' not found in the data")
            return []

    def filter_by_products(products_df, selected_products):
        """Filter products dataframe by selected products"""
        if not selected_products:
            return products_df
       
        return products_df[products_df['sku_level1'].isin(selected_products)]

    def merge_with_psid(products_df):
        """Merge products dataframe with P_SID from main database"""
        try:
            psid_df = get_psid()  # Get IIN to P_SID mapping
            merged_df = pd.merge(products_df, psid_df, on='IIN', how='inner')
            return merged_df
        except Exception as e:
            st.error(f"Error merging with P_SID: {e}")
            return products_df

    data_source = st.radio(
        "Выберите источник данных для UserID:",
        ('РБ Автоматический запуск', 'Гиперперсонализация', 'Продуктовая выборка')
    )

    excel_toggle = st.toggle("Excel")

    tables = ['DSSB_OCDS.mb11_global_control', 'ACRM_DW.RB_BLACK_LIST@ACRM', 'dssb_de.dim_clients_black_list', 'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK', 'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK',  'dssb_app.not_recommend_credits', 'BL_No_worker', 'dssb_app.abc_nbo_only', 'dssb_app.abc_ptb_models', 'dssb_app.abc_nbo_and_market']
    #selected_tables = st.multiselect('Select tables to query:', tables)

#'DSSB_OCDS.mb21_local_control', 'DSSB_OCDS.mb22_local_target'

    #st.sidebar.write(f'Вы вошли как: {st.session_state['username']}')
   
    # Initialize session state
    if 'open_new_tab' not in st.session_state:
        st.session_state.open_new_tab = False
   
    def set_open_new_tab():
        st.session_state.open_new_tab = True
   
    #st.button("AI helper", on_click=set_open_new_tab)
   
    # Check if we should open a new tab
    if st.session_state.open_new_tab:
        js = """
        <script>
        window.open('http://172.28.80.18:8525/&#39;, '_blank');
        </script>
        """
        st.components.v1.html(js, height=0, width=0)
        st.session_state.open_new_tab = False

   
    selected_tables = []

    checker = st.sidebar.toggle('Кто запустил кампанию?')
    with st.sidebar:
        if checker:
            main_checker()
           

    st.sidebar.write("-------------------------------")
    st.sidebar.title("Стоп листы")

    for table in tables:
        is_selected = st.sidebar.toggle(table, key=table, value = 1)

        if is_selected:
            selected_tables.append(table)

    st.sidebar.write("-------------------------------")
   
    pushToggled = st.sidebar.toggle('Push отключен')

    Extra_Filters = st.sidebar.toggle('Extra_Filters')

    device_filtered = st.sidebar.toggle('Device')

    if device_filtered:

        devices = ['android', 'iOS']

        selected_devices = []

        for device in devices:
            is_selected_devices = st.sidebar.checkbox(device, key=device)

            if is_selected_devices:
                selected_devices.append(device)


    if Extra_Filters:

        extra_streams = ['Переводы', 'Платежи', 'Kino.kz', 'Безопасность', 'Маркет', 'OTP/3ds/пароли', 'Инвестиции', 'Кредиты', 'Предложения Банка', 'Депозиты', 'Onlinebank', 'Halyk страхование', 'Homebank', 'Бонусы', 'Госуслуги', 'Operation failed HalykTravel', 'Общий пуш', 'Oперации по счетам']

        selected_extra_streams = []

        for extra_stream in extra_streams:
            is_selected_extra_stream = st.sidebar.checkbox(extra_stream, key=extra_stream)

            if is_selected_extra_stream:
                selected_extra_streams.append(extra_stream)

   
    st.sidebar.write("-------------------------------")
    local_control = st.sidebar.toggle('DSSB_OCDS.mb21_local_control')
    local_target = st.sidebar.toggle('DSSB_OCDS.mb22_local_target')

    if local_control or local_target:

        streams = ['market', 'general', 'travel', 'govtech', 'credit', 'insurance', 'deposit', 'kino', 'transactions', 'hm']
   
        selected_streams = []
       
        for stream in streams:
            is_selected = st.sidebar.checkbox(stream, key=stream)
   
            if is_selected:
                selected_streams.append(stream)



    st.sidebar.write("-------------------------------")
    local_control_rb3 = st.sidebar.toggle('DSSB_OCDS.mb21_local_control_RB3')
    local_target_rb3 = st.sidebar.toggle('DSSB_OCDS.mb22_local_target_RB3')

    if local_control_rb3 or local_target_rb3:

        streams_rb3 = ['бзк', 'бизнес и налоги', 'concert', 'недвижимость', 'transfers', 'все', 'aqyl', 'общая', 'транспорт', 'bnpl', 'card', 'realtime', 'theatr', 'здоровья', 'плашеты', 'cards', 'нотариус и цон', 'льготы, пособие, пенсии', 'halykapp', 'автокредит', 'ипотека', 'kino', 'цифровые документы']
   
        selected_streams_rb3 = []
       
        for stream_rb3 in streams_rb3:
            is_selected_rb3 = st.sidebar.checkbox(stream_rb3, key=stream_rb3)
   
            if is_selected_rb3:
                selected_streams_rb3.append(stream_rb3)

    if 'df' not in st.session_state:
        st.session_state.df = pd.DataFrame()

    if 'dfnew' not in st.session_state:
        st.session_state.dfnew = 0

    if 'dfdelta' not in st.session_state:
        st.session_state.dfdelta = 0

    if 'load' not in st.session_state:
        st.session_state.load = pd.DataFrame()

    with st.sidebar:

        if not st.session_state.df.empty:
            num_rows = st.session_state.df .shape[0]

            if st.session_state.dfnew != num_rows:
                st.session_state.dfdelta = num_rows - st.session_state.dfnew
                st.session_state.dfnew = num_rows

            value_main = f"{st.session_state.dfnew:,}"
            value_sub = f"{st.session_state.dfdelta:,}"
           
            st.metric(label="Записи в базе:", value=value_main, delta=value_sub)
           
        else:
            st.metric(label="Записи в базе:", value="No Data", delta="No Data")

        style_metric_cards()

        if st.button('Обновить виджет'):
            st.rerun()

    if excel_toggle:
        loaded_file = st.file_uploader("Загрузите файл данных (excel, csv)", type=['xlsx', 'csv'])

        if loaded_file:
            file_extension = loaded_file.name.split('.')[-1].lower()
           
            if file_extension == "csv":
                st.session_state.load = pd.read_csv(loaded_file)
            else:
                st.session_state.load = pd.read_excel(loaded_file)

            st.session_state.load['IIN'] = st.session_state.load['IIN'].astype(str).str.replace(',', '')

        check_password_for_sql = '123'
       
        password_for_sql = st.text_input("password")
       
        if password_for_sql == check_password_for_sql:
            sql_script = st.text_area("Sql to execute for(DSSB): ")

            if st.button('Load data from sql') and sql_script:

                with get_connection_DSSB_OCDS() as conn:
                    st.session_state.load = fetch_data_from_sql_query(conn, sql_script)


        if not st.session_state.load.empty:
            st.write(st.session_state.load.head(500))
            num_rows = st.session_state.load.shape[0]
            st.success(f"Выгруженно: {num_rows}")
       
   
   
    if data_source == 'РБ Автоматический запуск' and not excel_toggle:
        selected_info_columns = st.multiselect('Выберите фильтры', information_columns, default = ['SNAPSHOT_DATE', 'P_SID', 'IIN', 'PUBLIC_ID', 'IS_MAU'])
        #selected_sum_columns = []
        selected_sum_columns = st.multiselect('Выберите фильтры', sum_columns)
        #selected_stop_list_columns = st.multiselect('Выберите Стоп листы', ALL_STOP_LIST)
   
       
        #load_option = st.selectbox("Сколько загружать?:", ["Загрузить все", "Выбрать количество"])
       
        #if load_option == "Выбрать количество":
        #    user_limit = st.number_input("Выберите количество линий к загрузке:", min_value=1, value=20)
        #else:
        user_limit = None  # None or a special value indicating all records should be loaded
       
        #////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        # if statements for filters
        # Additional Filters based on user selection
        additional_filters = {}
        if 'AGE' in selected_info_columns:
            st.subheader('AGE')
            min_age, max_age = st.slider('Выберите возраст', 0, 100, (18, 60))
            additional_filters['AGE'] = ('BETWEEN', min_age, max_age)
       
       
       
       
        # Define a list of dictionaries containing column information
        info_columns = [
            {'column_name': 'LEGAL_FORM', 'display_name': 'LEGAL_FORM'},
            {'column_name': 'BUSINESS_TYPE', 'display_name': 'BUSINESS_TYPE'},
            {'column_name': 'BRANCH_NAME', 'display_name': 'BRANCH_NAME'},
            {'column_name': 'RISK_CATEGORY', 'display_name': 'RISK_CATEGORY'},
            {'column_name': 'OFFER', 'display_name': 'OFFER'},
            {'column_name': 'KATO', 'display_name': 'KATO'},
            {'column_name': 'BUSINESS_TYPE_REASON', 'display_name': 'BUSINESS_TYPE_REASON'},
            {'column_name': 'PREV_CAMP_ALL', 'display_name': 'PREV_CAMP_ALL'},
            {'column_name': 'PREV_CAMP_FIZ', 'display_name': 'PREV_CAMP_FIZ'},
            {'column_name': 'RANK_AQUIR', 'display_name': 'RANK_AQUIR'},
            {'column_name': 'RANK_FX', 'display_name': 'RANK_FX'}
   
   
        ]
       
        for column_info in info_columns:
            column_name = column_info['column_name']
            display_name = column_info['display_name']
           
            if column_name in selected_info_columns:
                st.subheader(display_name)
                with get_connection() as conn:
                    query = f'SELECT DISTINCT {column_name} FROM dssb_app.rb_feature_store WHERE {column_name} IS NOT NULL ORDER BY {column_name}'
                    results = pd.read_sql(query, conn)[column_name].tolist()
               
                selected_options = st.multiselect(f'Выберите {display_name}', options=results)
               
                if selected_options:
                    additional_filters[column_name] = ('IN', selected_options)
       
        #////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        # Initialize session state for storing the DataFrame if it's not already initialized
        if 'df' not in st.session_state:
            st.session_state.df = pd.DataFrame()

    # New data source: Product-based selection
    elif data_source == 'Продуктовая выборка' and not excel_toggle:
        st.subheader('Продуктовая выборка')
       
        # Initialize session state for products
        if 'products_df' not in st.session_state:
            st.session_state.products_df = pd.DataFrame()
       
        if st.button('Загрузить продуктовые данные'):
            st.session_state.products_df = load_products_from_parquet("final.parquet")
            if not st.session_state.products_df.empty:
                st.success(f"Загружено {len(st.session_state.products_df)} записей с продуктами")
                st.write("Превью данных:")
                st.write(st.session_state.products_df.head())
            else:
                st.error("Не удалось загрузить данные о продуктах")
       
        # Product selection interface
        if not st.session_state.products_df.empty:
            available_products = get_available_products(st.session_state.products_df)
           
            if available_products:
                selected_products = st.multiselect(
                    'Выберите продукты для кампании:',
                    options=available_products,
                    default=available_products[:3] if len(available_products) >= 3 else available_products,
                    help="Выберите один или несколько продуктов для таргетинга"
                )
               
                if selected_products:
                    # Filter by selected products
                    filtered_products_df = filter_by_products(st.session_state.products_df, selected_products)
                   
                    st.info(f"Выбрано {len(filtered_products_df)} клиентов с продуктами: {', '.join(selected_products)}")
                   
                    # Display statistics by product
                    if len(selected_products) > 1:
                        product_stats = filtered_products_df['sku_level1'].value_counts()
                        st.write("Распределение по продуктам:")
                        st.bar_chart(product_stats)
                   
                    # Store filtered data for further processing
                    st.session_state.filtered_products_df = filtered_products_df
                else:
                    st.warning("Выберите хотя бы один продукт для продолжения")
            else:
                st.error("Продукты не найдены в загруженных данных")
       
        # Set variables for compatibility with existing logic
        user_limit = None
        additional_filters = {}
        selected_info_columns = ['IIN', 'P_SID']  # Minimal required columns
        selected_sum_columns = []
       
        # Initialize session state for storing the DataFrame if it's not already initialized
        if 'df' not in st.session_state:
            st.session_state.df = pd.DataFrame()
           

    else:
        user_limit = None
        additional_filters = {}
        selected_info_columns = ['IIN', 'P_SID']
        selected_sum_columns = []
        if 'df' not in st.session_state:
            st.session_state.df = pd.DataFrame()
       
    if not excel_toggle:
       
        if st.button('Запрос к базе'):
            with st.status('Идет загрузка с базы, ожидайте...', expanded = True) as status:
               
                # Special handling for product-based data source
                if data_source == 'Продуктовая выборка':
                    if 'filtered_products_df' not in st.session_state or st.session_state.filtered_products_df.empty:
                        st.error("Сначала загрузите и выберите продукты!")
                        status.update(label='Ошибка: продукты не выбраны', state='error', expanded=False)
                        st.stop()
                   
                    # Use product-filtered data as the base
                    df = st.session_state.filtered_products_df.copy()
                    
                    # Ensure IIN consistency for product data
                    if 'IIN' in df.columns:
                        df['IIN'] = df['IIN'].astype(str).str.strip()
                   
                    # Merge with P_SID if not already present
                    if 'P_SID' not in df.columns:
                        df = merge_with_psid(df)
                        st.write('P_SID добавлен к продуктовым данным')
                   
                    st.write('Продуктовые данные загружены как основа')
                    user_ids = df['IIN'].tolist()
                    table_name = 'product_based'
                    user_id_col_name = 'IIN'
                   
                # Original logic for other data sources
                else:
                    if selected_tables == []:
                        st.error('🚨🚨🚨 Вы не выбрали ни одного Stop или Black листа! Вы уверенны? 🚨🚨🚨')

                        table_name = 'dssb_app.rb_feature_store'
                        user_id_col_name = 'IIN'
                        if user_limit != None:

                            all_columns = selected_info_columns + selected_sum_columns
                            #df = query_database(all_columns, additional_filters, user_limit)
                            df = pd.read_parquet('Databases/dssb_app.rb_feature_store.parquet')
                            df = df[all_columns]
                            st.write('Основная база подгружена')

                        if user_limit == None:
                            #st.info('1')
                            all_columns = selected_info_columns + selected_sum_columns
                            #df = query_database_all(all_columns, additional_filters)
                            df = pd.read_parquet('Databases/dssb_app.rb_feature_store.parquet')
                            df = df[all_columns]
                            st.write('Основная база подгружена')
                            #st.info('2')
                        user_ids = df['IIN'].tolist()

                        if selected_sum_columns != []:
                            # Fetch summed columns from SQL

                            summed_df = add_column_sum(df, selected_sum_columns)
                    else:
                        table_name = 'dssb_app.rb_feature_store'
                        user_id_col_name = 'IIN'
                        if user_limit != None:

                            all_columns = selected_info_columns + selected_sum_columns
                            #df = query_database(all_columns, additional_filters, user_limit)
                            df = pd.read_parquet('Databases/dssb_app.rb_feature_store.parquet')
                            df = df[all_columns]
                            st.write('Основная база подгружена')

                        if user_limit == None:
                            all_columns = selected_info_columns + selected_sum_columns
                            #df = query_database_all(all_columns, additional_filters)
                            df = pd.read_parquet('Databases/dssb_app.rb_feature_store.parquet')
                            df = df[all_columns]
                            st.write('Основная база подгружена')
                        user_ids = df['IIN'].tolist()

                        if selected_sum_columns != []:
                            # Fetch summed columns from SQL

                            summed_df = add_column_sum(df, selected_sum_columns)

                    if selected_sum_columns != []:
                        df = summed_df

                #st.write(df)

                num_rows = df.shape[0]

                if not selected_tables:

                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
                    st.session_state.df = df

               
   
                if selected_tables:
                    # Example usage within your main application
                    with get_connection_DSSB_OCDS() as conn:
                        #df = filter_dataframe(df, conn, selected_stop_list_columns,user_id_col_name, user_ids)
                        # Fetch and combine user IDs from selected tables
                        all_user_ids = pd.DataFrame()
                        for table in selected_tables:
                            fetched_ids = fetch_data(table)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids], ignore_index=True)
                            st.write(f'Стоп лист {table} очищен')

                        if local_control and not local_target:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_control(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_control {stream} очищен')

                        if local_target and not local_control:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_target(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_target {stream} очищен')

                        if local_control and local_target:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_control_target(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_control и local_target {stream} очищен')





                        if local_control_rb3 and not local_target_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_control_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_control {stream_rb3} очищен_rb3')

                        if local_target_rb3 and not local_control_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_target_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_target {stream_rb3} очищен_rb3')

                        if local_control_rb3 and local_target_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_control_target_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_control_rb3 и local_target_rb3 {stream_rb3} очищен')



                       

                        if pushToggled:
                            #st.info('toggled push')
                            fetched_ids_push = fetch_data_push()
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_push], ignore_index=True)
                            st.write('Push очищен')

                        if Extra_Filters:
                            #for extra_stream in selected_extra_streams:
                            fetched_ids_extra_filters = fetch_data_extra_filters(selected_extra_streams)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_filters], ignore_index=True)
                            st.write(f'Extra filters {selected_extra_streams} очищены')

                        if device_filtered:
                            #for device in selected_devices:
                            fetched_ids_devices = fetch_data_device(selected_devices)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_devices], ignore_index=True)  
                            st.write(f'Devise {selected_devices} очищен')
                           
                           
                       
                        # Remove duplicates across all fetched IDs
                        all_user_ids = all_user_ids.drop_duplicates()
       
                        # Assume df is defined elsewhere and has column 'IIN'
                        original_count = len(df)
                        df_filtered = df[~df['IIN'].isin(all_user_ids['IIN'])]
                        removed_count = original_count - len(df_filtered)
                       
                        st.success(f'Removed {removed_count} users from df.')
                                                   
                        num_rows = df_filtered.shape[0]
   
                   
                        st.success("Данные успешно загружены и обновлены из базы данных.")
                        st.success(f"В базе {num_rows} записей после блэк и стоп листов")
                        st.session_state.df = df_filtered

                        status.update(
                            label = 'База Готова.', state = 'complete', expanded = False
                        )

                        #df_filtered.to_parquet('Databases/DF_final.parquet', compression='snappy')

            #st.experimental_rerun()

    elif excel_toggle:
        if st.button('Очистка файла'):
            with st.status('Идет загрузка с базы, ожидайте...', expanded = True) as status:
                df = st.session_state.load

                st.write('Добавляем PSID')
                psid_df = get_psid()
                st.write('PSID добавлен')
                if selected_tables == []:
                    st.error('🚨🚨🚨 Вы не выбрали ни одного Stop или Black листа! Вы уверенны? 🚨🚨🚨')
   
                    table_name = 'dssb_app.rb_feature_store'
                    user_id_col_name = 'IIN'
                    #df = pd.read_excel(loaded_file)
                    original_count = len(df)
                    df['IIN'] = df['IIN'].astype(str).str.replace(',', '')
                    df = pd.merge(df, psid_df, on='IIN', how='inner')
                    user_ids = df['IIN'].tolist()
       
                else:
                    table_name = 'dssb_app.rb_feature_store'
                    user_id_col_name = 'IIN'
                    #df = pd.read_excel(loaded_file)
                    original_count = len(df)
                    df['IIN'] = df['IIN'].astype(str).str.replace(',', '')
                    df = pd.merge(df, psid_df, on='IIN', how='inner')
                    user_ids = df['IIN'].tolist()
   
                    num_rows = df.shape[0]
   
                if not selected_tables:

                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
                    st.session_state.df = df


   
                if selected_tables:
                    # Example usage within your main application
                    with get_connection_DSSB_OCDS() as conn:
                        #df = filter_dataframe(df, conn, selected_stop_list_columns,user_id_col_name, user_ids)
                        # Fetch and combine user IDs from selected tables
                        all_user_ids = pd.DataFrame()
                        for table in selected_tables:
                            fetched_ids = fetch_data(table)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids], ignore_index=True)
                            st.write(f'Стоп лист {table} очищен')

                        if local_control and not local_target:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_control(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_control {stream} очищен')

                        if local_target and not local_control:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_target(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_target {stream} очищен')

                        if local_control and local_target:
                            for stream in selected_streams:
                                fetched_ids_extra = fetch_data_stream_control_target(stream, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra], ignore_index=True)  
                                st.write(f'local_control и local_target {stream} очищен')








                        if local_control_rb3 and not local_target_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_control_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_control {stream_rb3} очищен_rb3')

                        if local_target_rb3 and not local_control_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_target_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_target {stream_rb3} очищен_rb3')

                        if local_control_rb3 and local_target_rb3:
                            for stream_rb3 in selected_streams_rb3:
                                fetched_ids_extra_rb3 = fetch_data_stream_control_target_rb3(stream_rb3, conn)
                                all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_rb3], ignore_index=True)  
                                st.write(f'local_control_rb3 и local_target_rb3 {stream_rb3} очищен')



                       

                        if pushToggled:
                            #st.info('toggled push')
                            fetched_ids_push = fetch_data_push()
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_push], ignore_index=True)
                            st.write('Push очищен')

                        if Extra_Filters:
                            #for extra_stream in selected_extra_streams:
                            fetched_ids_extra_filters = fetch_data_extra_filters(selected_extra_streams)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_extra_filters], ignore_index=True)
                            st.write(f'Extra filters {selected_extra_streams} очищены')

                        if device_filtered:
                            #for device in selected_devices:
                            fetched_ids_devices = fetch_data_device(selected_devices)
                            all_user_ids = pd.concat([all_user_ids, fetched_ids_devices], ignore_index=True)  
                            st.write(f'Devise {selected_devices} очищен')
                           
                           
                       
                        # Remove duplicates across all fetched IDs
                        all_user_ids = all_user_ids.drop_duplicates()
       
                        # Assume df is defined elsewhere and has column 'IIN'
                        df_filtered = df[~df['IIN'].isin(all_user_ids['IIN'])]
                        removed_count = original_count - len(df_filtered)
                       
                        st.success(f'Removed {removed_count} users from df.')
                                                   
                        num_rows = df_filtered.shape[0]
   
                   
                        st.success("Данные успешно загружены и обновлены из базы данных.")
                        st.success(f"В базе {num_rows} записей после блэк и стоп листов")
                        st.session_state.df = df_filtered

                        status.update(
                            label = 'База Готова.', state = 'complete', expanded = False
                        )

            #st.experimental_rerun()


    if not st.session_state.df.empty:
        st.write(st.session_state.df.head(500))
        num_rows = st.session_state.df.shape[0]
        st.success(f"В базе {num_rows}")

        def trim_dataframe(dataframe, max_rows):
            trimmed_df = dataframe.dropna().head(max_rows)
            return trimmed_df

        with st.expander(f"Очистка существующих кампаний"):
           
            prev_campaign = campaign_id = st.text_input("Номера кампаний к очистке")

            if st.button("Очистить клиентов"):
                st.write(prev_campaign)
                with get_connection_ED_OCDS() as conn:
                    all_user_ids_campaign = pd.DataFrame()
                    fetched_prev_campaign_users = fetch_data_prev_campaign(prev_campaign, conn)

                    all_user_ids_campaign = pd.concat([all_user_ids_campaign, fetched_prev_campaign_users], ignore_index=True)  

                    # Remove duplicates across all fetched IDs
                    all_user_ids_campaign = all_user_ids_campaign.drop_duplicates()
                    df = st.session_state.df

                    # Assume df is defined elsewhere and has column 'IIN'
                    original_count = len(df)
                    df_filtered = df[~df['IIN'].isin(all_user_ids_campaign['CUSTOMER_IIN'])]
                    removed_count = original_count - len(df_filtered)
                   
                    st.success(f'Removed {removed_count} users from df.')
                                               
                    num_rows = df_filtered.shape[0]

               
                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей после блэк и стоп листов")
                    st.session_state.df = df_filtered


           

        with st.expander(f"Чистка Уникальности"):
            DATE_FOR_CLEAN = st.date_input("Очистка по дате:")

            if st.button("Очистить тех кто в базе campaigns_users > 1 раз"):
                all_user_ids = pd.DataFrame()
                original_count = len(st.session_state.df)
                with get_connection_SPSS_OCDS() as conn:
                    fetched_ids_more_than_one = fetch_data_base_more_than_one(conn, DATE_FOR_CLEAN)
                    all_user_ids = pd.concat([all_user_ids, fetched_ids_more_than_one], ignore_index=True)

                    all_user_ids = all_user_ids.drop_duplicates()

                    # Enhanced IIN comparison to handle data type consistency issues
                    # Convert both dataframes' IIN columns to string format for consistent comparison
                    if not all_user_ids.empty:
                        st.info(f"Найдено {len(all_user_ids)} дублированных IIN в базе на {DATE_FOR_CLEAN}")
                        
                        # Normalize IINs in fetched data to strings without leading zeros for comparison
                        all_user_ids['IIN_normalized'] = all_user_ids['IIN'].astype(str).str.strip().str.lstrip('0')
                        
                        # Normalize IINs in session dataframe to strings without leading zeros
                        st.session_state.df['IIN_normalized'] = st.session_state.df['IIN'].astype(str).str.strip().str.lstrip('0')
                        
                        # Filter using normalized IINs
                        df_filtered = st.session_state.df[~st.session_state.df['IIN_normalized'].isin(all_user_ids['IIN_normalized'])]
                        
                        # Remove the temporary normalized column
                        df_filtered = df_filtered.drop('IIN_normalized', axis=1)
                        st.session_state.df = df_filtered
                    else:
                        st.info(f"Не найдено дублированных IIN в базе на {DATE_FOR_CLEAN}")
                        # If no duplicates found, no filtering needed
                        df_filtered = st.session_state.df

                    num_rows = df_filtered.shape[0]

                    removed_count = original_count - len(df_filtered)
                           
                    st.info(f'удаленно {removed_count} из базы на {DATE_FOR_CLEAN}.')

               
                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
   
            if st.button('Записи в базе fd_rb2_campaigns_users:'):
                with get_connection_SPSS_OCDS() as conn:
                    fetched_ids_more_than_one = fetch_data_base_more_than_one(conn, DATE_FOR_CLEAN)
                num = len(fetched_ids_more_than_one)
                value_main = f"{num:,}"
                st.metric(label=f"Записи в базе на {DATE_FOR_CLEAN}:", value=value_main)
   
   
           
            style_metric_cards()
           
        with st.expander("Очистка"):

            # Button to remove rows where IS_MAU is 0
            if st.button('Очистить всех где MAU = 0  из dssb_dev.dssb_push_analytics'):
                with get_connection_DSSB_OCDS() as conn:
                    st.session_state.df = filter_df_by_iin(st.session_state.df)
       
                    num_rows = st.session_state.df.shape[0]
                    st.success("MAU успешно обновлен")
                    st.success(f"В базе {num_rows} записей")
                    st.write(st.session_state.df.head(500))
                    #st.experimental_rerun()
           
            # Button to remove rows where PUBLIC_ID is None or empty
            if st.button('Очистить всех где PUBLIC_ID = 0 или None'):
                st.session_state.df = st.session_state.df[st.session_state.df['PUBLIC_ID'].notna() & (st.session_state.df['PUBLIC_ID'] != '') & (st.session_state.df['PUBLIC_ID'] != 'None')]
                st.dataframe(st.session_state.df)
   
                num_rows = st.session_state.df.shape[0]
                st.success("PUBLIC_ID успешно обновлен")
                st.success(f"В базе {num_rows} записей")
                #st.experimental_rerun()

            if st.button('Сколько в покрытие МАU?'):
                parquet_file = 'Databases/MAU.parquet'
                df_mau = pd.read_parquet(parquet_file)
                num_rows = df_mau.shape[0]
                st.info(f"В базе MAU {num_rows} записей")

            if st.button('Совместить выборку с покрытием MAU'):
                parquet_file = 'Databases/MAU.parquet'
                df_mau = pd.read_parquet(parquet_file)
                iins_to_keep = set(df_mau['IIN'])
                st.session_state.df = st.session_state.df[st.session_state.df['IIN'].isin(iins_to_keep)]
                num_rows = st.session_state.df.shape[0]
                st.success(f"В базе {num_rows} записей")

            st.write('-----------------------------------------------------------')


            filial_list = [
'Туркестанский ОФ'
,'Павлодарский ОФ'
,'Северо-Казахстанский ОФ'
,'РФ Семей'
,'Акмолинский ОФ'
,'Шымкентский городской филиал'
,'Астанинский городской филиал'
,'Жанаозенский РФ'
,'Алматинский ОФ'
,'Жезказганский РФ'
,'не заполнено'
,'Экибастузский РФ'
,'Костанайский ОФ'
,'Темиртауский РФ'
,'Байконырский РФ'
,'Жамбылский ОФ'
,'Карагандинский ОФ'
,'АО Народный Банк Республики Казахстан'
,'Шымкентский региональный филиал'
,'Актюбинский ОФ'
,'Алматинский областной филиал г.Конаев'
,'ОФ «Абай»'
,'Западно-Казахстанский ОФ'
,'ОФ «Ұлытау»'
,'Балхашский РФ'
,'Талдыкорганский ОФ'
,'ОФ «Жетісу»'
,'Атырауский ОФ'
,'Мангистауский ОФ'
,'Астанинский РФ'
,'Алматинский ГФ'
,'Восточно-Казахстанский ОФ'
,'Кызылординский ОФ'
,'Головной Банк'
,'Семипалатинский РФ'
]
            st.subheader('Фильтр для филиалов')
            selected_filials = st.multiselect("Выбериет филиалы к очистке:", filial_list)

            if 'backup' not in st.session_state or st.session_state['backup'].empty:
                st.session_state['backup'] = pd.DataFrame()

            col1, col2 = st.columns(2)

            with col1:

                if st.button('Filter филиалы'):

                    st.session_state['backup'] = st.session_state.df
                   
                    with get_connection_DSSB_OCDS() as conn:
                        df_filials = get_iin_bin_for_filials(conn, selected_filials)

                        iins_to_keep = set(df_filials['IIN'])
                   
                        # Filter df1 to keep only rows where IIN is in df2
                        st.session_state.df = st.session_state.df[st.session_state.df['IIN'].isin(iins_to_keep)]
                        st.write(st.session_state.df.head(500))
                        num_rows = st.session_state.df.shape[0]
                       
   
                   
                        st.success("Данные успешно загружены и обновлены из базы данных.")
                        st.success(f"В базе {num_rows} записей")
                        #st.experimental_rerun()

            with col2:

                if st.button('Вернуть базу до фильтра филиалов') and not st.session_state['backup'].empty:
                    st.session_state.df = st.session_state['backup']
                    st.success('база востановленна в состояние до фильтра филиалов')

                    st.write(st.session_state.df.head(500))
                    num_rows = st.session_state.df.shape[0]
                   

               
                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
                    #st.experimental_rerun()
               
            st.write('-----------------------------------------------------------')
            st.subheader('Фильтр для возраста и пола')
            st.subheader('Примерное время фильтрации 5 мин')
            min_age = st.number_input('Enter minimum age', min_value=0, max_value=120, step=1, value=0)
            max_age = st.number_input('Enter maximum age', min_value=0, max_value=120, step=1, value=120)
            sex_selection = st.multiselect('Select sex type', options=['M', 'F'], default=['M', 'F'])

            col3, col4 = st.columns(2)

            with col3:
                if st.button('Filter DataFrame'):

                    st.session_state['backup'] = st.session_state.df
                   
                    with get_connection_DSSB_OCDS() as conn:
                        df2 = load_sql_table(conn)
                        st.write('got df')
                        filtered_df2 = filter_dataframe_extra(df2, min_age, max_age, sex_selection)
               
                        st.session_state.df = filter_df1_based_on_df2(st.session_state.df, filtered_df2)
                        st.write("Filtered df1 based on df2:")
                        st.write(st.session_state.df.head(500))
                        num_rows = st.session_state.df.shape[0]
       
                   
                        st.success(f"Данные успешно обновлены")
                        st.success(f"В базе {num_rows} записей очистки")
                        #st.experimental_rerun()
            with col4:
                if st.button('Вернуть базу до фильтра возраста и пола') and not st.session_state['backup'].empty:
                    st.session_state.df = st.session_state['backup']
                    st.success('база востановленна в состояние до фильтра возраста и пола')

                    st.write(st.session_state.df.head(500))
                    num_rows = st.session_state.df.shape[0]
                   

               
                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
                    #st.experimental_rerun()

            st.write('-----------------------------------------------------------')
            st.subheader('Очистка сумм')
            user_input = st.number_input("Выберите сумму:")

            col5, col6 = st.columns(2)

            with col5:
                if st.button('Очистить всех с суммой меньше') and user_input:
                    st.session_state['backup'] = st.session_state.df
                    st.session_state.df = st.session_state.df[st.session_state.df['Column_sum'] >= user_input]
                    st.write(st.session_state.df.head(500))
                                               
                    num_rows = st.session_state.df.shape[0]
   
               
                    st.success(f"Данные успешно обновлены, все с суммой менеше чем {user_input} удаленны")
                    st.success(f"В базе {num_rows} записей после блэк и стоп листов")
                    #st.experimental_rerun()
            with col6:
                if st.button('Вернуть базу до фильтра суммы') and not st.session_state['backup'].empty:
                    st.session_state.df = st.session_state['backup']
                    st.success('база востановленна в состояние до фильтра суммы')

                    st.write(st.session_state.df.head(500))
                    num_rows = st.session_state.df.shape[0]
                   

               
                    st.success("Данные успешно загружены и обновлены из базы данных.")
                    st.success(f"В базе {num_rows} записей")
                    #st.experimental_rerun()

            st.write('-----------------------------------------------------------')
            st.subheader('Укоротить базу')
            max_rows = st.number_input("Выберите количество линий:", min_value=1, value=20)
           
            # Button to trim the DataFrame
            if st.button('Укоротить выборку'):
                st.session_state.df = trim_dataframe(st.session_state.df, max_rows)
                num_rows = st.session_state.df.shape[0]
                st.write(f"Trimmed DataFrame (first {max_rows} rows)")
                st.success(f"В базе {num_rows} записей")
                st.write(st.session_state.df)
                #st.experimental_rerun()
               
               
   
        CAMPAIGNCODE_var = None
        stratify_check = None
        jira_check = None
       
        if not st.session_state.df.empty:
            stratify_check = st.toggle('Стратифицировать DF?')
            jira_check = st.toggle('Jira')

        if jira_check:
            if 'df' not in st.session_state or st.session_state.df.empty:
                st.session_state.df = pd.DataFrame()
            jira_main()
            st.write('-----------------------------------------------------------')
       
        if stratify_check:
            # Initialize session state
            if 'open_new_tab' not in st.session_state:
                st.session_state.open_new_tab = False
           
            def set_open_new_tab():
                st.session_state.open_new_tab = True
           
            st.button("Stratification", on_click=set_open_new_tab)

            if st.button('Добавить Продукты'):

                products = pd.read_parquet('Databases/dssb_app.products_per_fl_prod.parquet')
                st.write(products.head(500))

                st.session_state.df = pd.merge(st.session_state.df, products, on = 'IIN', how = 'inner')
           
            # Check if we should open a new tab
            if st.session_state.open_new_tab:
                js = """
                <script>
                window.open('http://172.28.80.18:8525/&#39;, '_blank');
                </script>
                """
                st.components.v1.html(js, height=0, width=0)
                st.session_state.open_new_tab = False
           
           
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(st.session_state.df)
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)
            st.download_button(
                label=f"Скачать Разбиение как Parquet",
                data=buffer,
                file_name=f'DF.parquet',
                mime='application/octet-stream',
            )



        #////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        # Loading data into Table


        def display_upload_status():
            """Display current upload readiness status to users"""
            if 'df' not in st.session_state or st.session_state.df.empty:
                st.warning("⚠️ **Статус готовности:** Данные не загружены")
                return False
            
            if 'saved_data' not in st.session_state or st.session_state.saved_data != 1:
                st.warning("⚠️ **Статус готовности:** Метаданные кампании не сохранены")
                return False
                
            missing_fields = []
            if not st.session_state.get('CAMPAIGNCODE'):
                missing_fields.append('CAMPAIGNCODE')
            if not st.session_state.get('STREAM'):
                missing_fields.append('STREAM')
            if not st.session_state.get('SHORT_DESC'):
                missing_fields.append('SHORT_DESC')
                
            if missing_fields:
                st.error(f"❌ **Статус готовности:** Не заполнены обязательные поля: {', '.join(missing_fields)}")
                return False
            else:
                st.success("✅ **Статус готовности:** Все проверки пройдены, готово к загрузке")
                return True


        def safe_database_upload_with_skip(df, get_connection_func, table_name, column_mapping, additional_info, operation_name, allow_skip=True):
            """
            Safely perform database upload with ability to skip problematic records
            """
            try:
                # Pre-upload validation
                if df is None or df.empty:
                    st.error("❌ Нет данных для загрузки. Проверьте выборку клиентов.")
                    return False, []
                
                # Show what we're about to do
                with st.spinner(f'Загружаем данные в {table_name}...'):
                    # Get database connection
                    try:
                        conn = get_connection_func()
                    except Exception as conn_error:
                        st.error(f"❌ Ошибка подключения к базе данных: Не удалось подключиться. Попробуйте еще раз или обратитесь к администратору.")
                        logging.error(f"Connection error for {operation_name}: {conn_error}")
                        return False, []
                    
                    # Choose the appropriate upload function
                    if allow_skip and len(df) > 1:  # Use skip function for bulk uploads
                        success, message, records_count, failed_records = insert_dataframe_to_oracle_with_skip(
                            df, conn, table_name, column_mapping, additional_info
                        )
                    else:  # Use original function for single records or when skip is disabled
                        success, message, records_count = insert_dataframe_to_oracle(
                            df, conn, table_name, column_mapping, additional_info
                        )
                        failed_records = []
                    
                    # Display result to user
                    if success:
                        st.success(message)
                        
                        # Show detailed results if there were failed records
                        if failed_records:
                            with st.expander(f"⚠️ Подробности: {len(failed_records)} записей пропущено", expanded=False):
                                st.write("**Причины пропуска записей:**")
                                
                                # Group by error type
                                error_groups = {}
                                for failed in failed_records:
                                    error_type = failed['error']
                                    if error_type not in error_groups:
                                        error_groups[error_type] = []
                                    error_groups[error_type].append(failed)
                                
                                for error_type, records in error_groups.items():
                                    st.write(f"- **{error_type}**: {len(records)} записей")
                                    if len(records) <= 5:  # Show details for small numbers
                                        for record in records:
                                            st.write(f"  - Строка {record['row_index']}: {record['data']}")
                                    elif len(records) <= 20:
                                        st.write(f"  - Строки: {', '.join([str(r['row_index']) for r in records])}")
                                    else:
                                        st.write(f"  - Множественные записи (всего {len(records)})")
                        
                        logging.info(f"Successful {operation_name}: {records_count} records loaded, {len(failed_records)} skipped")
                        return True, failed_records
                    else:
                        st.error(message)
                        if records_count > 0:
                            st.warning(f"⚠️ Частичная загрузка: {records_count} записей было загружено до критической ошибки.")
                        
                        # Show failed records if any
                        if failed_records:
                            with st.expander(f"❌ Проблемные записи: {len(failed_records)}", expanded=True):
                                for failed in failed_records[:10]:  # Show first 10
                                    st.write(f"Строка {failed['row_index']}: {failed['error']}")
                                if len(failed_records) > 10:
                                    st.write(f"... и еще {len(failed_records) - 10} записей")
                        
                        logging.error(f"Failed {operation_name}: {message}")
                        return False, failed_records
                        
            except Exception as e:
                st.error(f"❌ Критическая ошибка при загрузке в {table_name}: {str(e)[:200]}...")
                logging.error(f"Critical error in {operation_name}: {e}")
                return False, []


        def safe_database_upload(df, get_connection_func, table_name, column_mapping, additional_info, operation_name):
            """
            Safely perform database upload with comprehensive error handling and user feedback
            """
            try:
                # Pre-upload validation
                if df is None or df.empty:
                    st.error("❌ Нет данных для загрузки. Проверьте выборку клиентов.")
                    return False
                
                # Show what we're about to do
                with st.spinner(f'Загружаем данные в {table_name}...'):
                    # Get database connection
                    try:
                        conn = get_connection_func()
                    except Exception as conn_error:
                        st.error(f"❌ Ошибка подключения к базе данных: Не удалось подключиться. Попробуйте еще раз или обратитесь к администратору.")
                        logging.error(f"Connection error for {operation_name}: {conn_error}")
                        return False
                    
                    # Perform the upload
                    success, message, records_count = insert_dataframe_to_oracle(
                        df, conn, table_name, column_mapping, additional_info
                    )
                    
                    # Display result to user
                    if success:
                        st.success(message)
                        logging.info(f"Successful {operation_name}: {records_count} records")
                        return True
                    else:
                        st.error(message)
                        if records_count > 0:
                            st.warning(f"⚠️ Частичная загрузка: {records_count} записей было загружено до ошибки.")
                        logging.error(f"Failed {operation_name}: {message}")
                        return False
                        
            except Exception as e:
                st.error(f"❌ Критическая ошибка при загрузке в {table_name}: {str(e)[:200]}...")
                logging.error(f"Critical error in {operation_name}: {e}")
                return False


        def insert_dataframe_to_oracle(df, conn, table_name, column_mapping, additional_info, batch_size=10000):
            """
            Insert dataframe to Oracle database with comprehensive error handling
            Returns: (success: bool, error_message: str, records_inserted: int)
            """
            cursor = None
            records_inserted = 0
            
            try:
                # Pre-validation checks
                if df is None or df.empty:
                    return False, "❌ Ошибка: Нет данных для загрузки. Проверьте выборку клиентов.", 0
                
                if not conn:
                    return False, "❌ Ошибка подключения: Не удалось подключиться к базе данных. Попробуйте еще раз.", 0
                
                cursor = conn.cursor()
       
                # Create a set of all keys to ensure uniqueness
                all_keys = list(set(column_mapping.values()) | set(additional_info.keys()))
        
                # Construct the fields string dynamically, ensuring uniqueness
                fields = ', '.join(all_keys)
        
                # Prepare the insert query
                insert_query = f"INSERT INTO {table_name} ({fields}) VALUES ({', '.join([':' + str(i+1) for i in range(len(all_keys))])})"
        
                # Prepare data for insertion
                data_to_insert = []
                for _, row in df.iterrows():
                    row_data = []
                    for key in all_keys:
                        if key in column_mapping.values():
                            original_col = next(col for col, mapped_col in column_mapping.items() if mapped_col == key)
                            row_data.append(row[original_col] if original_col in df.columns else None)
                        else:
                            row_data.append(additional_info.get(key))
                    data_to_insert.append(row_data)
        
                total_records = len(data_to_insert)
                num_batches = (total_records + batch_size - 1) // batch_size
        
                # Initialize Streamlit progress bar
                progress_bar = st.progress(0)
                progress_text = st.empty()
       
                # Batch insert with progress bar
                for i in range(num_batches):
                    start_idx = i * batch_size
                    end_idx = min(start_idx + batch_size, total_records)
                    batch = data_to_insert[start_idx:end_idx]
                    
                    try:
                        cursor.executemany(insert_query, batch)
                        conn.commit()
                        records_inserted += len(batch)
       
                        # Update progress bar
                        progress = (end_idx / total_records)
                        progress_bar.progress(progress)
                        progress_text.text(f"Загружено {records_inserted}/{total_records} записей... {int(progress * 100)}%")
       
                    except Exception as batch_error:
                        conn.rollback()
                        error_msg = str(batch_error).lower()
                        
                        # Handle specific Oracle errors with user-friendly messages
                        if "unique constraint" in error_msg or "ora-00001" in error_msg:
                            return False, f"❌ Ошибка дублирования: Найдены дубликаты в данных. Возможно кампания уже была загружена ранее. Проверьте CAMPAIGNCODE или очистите дубликаты.", records_inserted
                        elif "foreign key constraint" in error_msg or "ora-02291" in error_msg:
                            return False, f"❌ Ошибка связи данных: Некорректные данные кампании. Проверьте CAMPAIGNCODE и метаданные кампании.", records_inserted
                        elif "insufficient privileges" in error_msg or "ora-01031" in error_msg:
                            return False, f"❌ Ошибка доступа: Недостаточно прав для записи в таблицу {table_name}. Обратитесь к администратору.", records_inserted
                        elif "tablespace" in error_msg or "ora-01654" in error_msg:
                            return False, f"❌ Ошибка места: Недостаточно места в базе данных. Обратитесь к администратору.", records_inserted
                        elif "table or view does not exist" in error_msg or "ora-00942" in error_msg:
                            return False, f"❌ Ошибка схемы: Таблица {table_name} не найдена. Обратитесь к администратору.", records_inserted
                        elif "too many values" in error_msg or "not enough values" in error_msg:
                            return False, f"❌ Ошибка формата данных: Несоответствие колонок данных. Проверьте структуру выборки.", records_inserted
                        else:
                            return False, f"❌ Ошибка базы данных: {str(batch_error)[:200]}... Обратитесь к администратору если проблема повторяется.", records_inserted
           
                progress_bar.empty()
                progress_text.empty()
                
                return True, f"✅ Успешно загружено {records_inserted} записей в {table_name}", records_inserted
                
            except Exception as e:
                if conn:
                    conn.rollback()
                    
                error_msg = str(e).lower()
                
                # Handle connection and general errors
                if "network" in error_msg or "connection" in error_msg or "ora-12170" in error_msg or "ora-12154" in error_msg:
                    return False, "❌ Ошибка сети: Проблема с подключением к базе данных. Проверьте интернет соединение и попробуйте еще раз.", records_inserted
                elif "timeout" in error_msg or "ora-01013" in error_msg:
                    return False, "❌ Тайм-аут: Операция заняла слишком много времени. Попробуйте уменьшить размер выборки.", records_inserted
                elif "memory" in error_msg or "ora-04030" in error_msg:
                    return False, "❌ Ошибка памяти: Недостаточно памяти для обработки данных. Попробуйте уменьшить размер выборки.", records_inserted
                else:
                    return False, f"❌ Неожиданная ошибка: {str(e)[:200]}... Обратитесь к администратору.", records_inserted
                    
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                # Clean up progress indicators
                try:
                    progress_bar.empty()
                    progress_text.empty()
                except:
                    pass
                return False, f"❌ Неожиданная ошибка: {str(e)[:200]}... Обратитесь к администратору.", records_inserted


        def insert_dataframe_to_oracle_with_skip(df, conn, table_name, column_mapping, additional_info, batch_size=10000):
            """
            Insert dataframe to Oracle database with ability to skip problematic records
            Returns: (success: bool, message: str, successful_records: int, failed_records: list)
            """
            cursor = None
            successful_records = 0
            failed_records = []
            total_records = len(df)
            
            try:
                # Pre-validation checks
                if df is None or df.empty:
                    return False, "❌ Ошибка: Нет данных для загрузки. Проверьте выборку клиентов.", 0, []
                
                if not conn:
                    return False, "❌ Ошибка подключения: Не удалось подключиться к базе данных. Попробуйте еще раз.", 0, []
                
                cursor = conn.cursor()
           
                # Create a set of all keys to ensure uniqueness
                all_keys = list(set(column_mapping.values()) | set(additional_info.keys()))
           
                # Construct the fields string dynamically, ensuring uniqueness
                fields = ', '.join(all_keys)
           
                # Prepare the insert query
                insert_query = f"INSERT INTO {table_name} ({fields}) VALUES ({', '.join([':' + str(i+1) for i in range(len(all_keys))])})"
           
                # Prepare data for insertion
                data_to_insert = []
                for idx, (_, row) in enumerate(df.iterrows()):
                    row_data = []
                    for key in all_keys:
                        if key in column_mapping.values():
                            original_col = next(col for col, mapped_col in column_mapping.items() if mapped_col == key)
                            row_data.append(row[original_col] if original_col in df.columns else None)
                        else:
                            row_data.append(additional_info.get(key))
                    data_to_insert.append((idx, row_data))
           
                # Initialize Streamlit progress indicators
                progress_bar = st.progress(0)
                progress_text = st.empty()
                status_text = st.empty()
           
                # Try batch insert first, fall back to individual records if needed
                def try_batch_insert(batch_data):
                    nonlocal successful_records, failed_records
                    
                    try:
                        # Try to insert the entire batch
                        batch_rows = [item[1] for item in batch_data]
                        cursor.executemany(insert_query, batch_rows)
                        conn.commit()
                        successful_records += len(batch_data)
                        return True
                    except Exception as batch_error:
                        # Batch failed, try individual records
                        conn.rollback()
                        error_msg = str(batch_error).lower()
                        
                        # If it's a uniqueness error, process records individually
                        if "unique" in error_msg or "ora-20001" in error_msg or "ora-00001" in error_msg:
                            for idx, row_data in batch_data:
                                try:
                                    cursor.execute(insert_query, row_data)
                                    conn.commit()
                                    successful_records += 1
                                except Exception as row_error:
                                    conn.rollback()
                                    row_error_msg = str(row_error)
                                    
                                    # Create readable error message
                                    if "unique" in row_error_msg.lower() or "ora-20001" in row_error_msg.lower():
                                        error_desc = "Дубликат записи"
                                    elif "ora-02291" in row_error_msg.lower():
                                        error_desc = "Нарушение связи данных"
                                    else:
                                        error_desc = f"Ошибка данных: {row_error_msg[:100]}"
                                    
                                    failed_records.append({
                                        'row_index': idx,
                                        'error': error_desc,
                                        'data': row_data[:2] if len(row_data) >= 2 else row_data  # Show first 2 fields for identification
                                    })
                            return True
                        else:
                            # Different type of error, re-raise
                            raise batch_error
                
                # Process data in batches
                num_batches = (total_records + batch_size - 1) // batch_size
                
                for batch_idx in range(num_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, total_records)
                    batch = data_to_insert[start_idx:end_idx]
                    
                    try_batch_insert(batch)
                    
                    # Update progress
                    progress = (end_idx / total_records)
                    progress_bar.progress(progress)
                    progress_text.text(f"Обработано {end_idx}/{total_records} записей...")
                    
                    if failed_records:
                        status_text.text(f"✅ Загружено: {successful_records} | ⚠️ Пропущено: {len(failed_records)}")
                    else:
                        status_text.text(f"✅ Загружено: {successful_records}")
           
                # Clean up progress indicators
                progress_bar.empty()
                progress_text.empty()
                status_text.empty()
                
                # Create result message
                if successful_records == total_records:
                    message = f"✅ Успешно загружено все {successful_records} записей в {table_name}"
                elif successful_records > 0:
                    message = f"⚠️ Частичная загрузка: {successful_records} из {total_records} записей загружено в {table_name}. Пропущено {len(failed_records)} дубликатов/проблемных записей."
                else:
                    message = f"❌ Загрузка не удалась: все {total_records} записей имеют проблемы"
                
                return successful_records > 0, message, successful_records, failed_records
                
            except Exception as e:
                if conn:
                    conn.rollback()
                    
                error_msg = str(e).lower()
                
                # Handle connection and general errors
                if "network" in error_msg or "connection" in error_msg:
                    return False, "❌ Ошибка сети: Проблема с подключением к базе данных.", successful_records, failed_records
                elif "timeout" in error_msg:
                    return False, "❌ Тайм-аут: Операция заняла слишком много времени.", successful_records, failed_records
                else:
                    return False, f"❌ Критическая ошибка: {str(e)[:200]}...", successful_records, failed_records
                    
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                # Clean up progress indicators
                try:
                    progress_bar.empty()
                    progress_text.empty()
                    status_text.empty()
                except:
                    pass


#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
       
        # Start of Streamlit UI
        if 'df' not in st.session_state or st.session_state.df.empty:
            st.warning("Данные не загружены. Пожалуйста выберите данные к загрузке")
        else:
            table_name = 'dssb_ocds.mb01_camp_dict'
            campaign_id = st.text_input("Campaign ID:")

            if 'STREAM' not in st.session_state or st.session_state.df.empty:
                STREAM_var = None
            else:
                STREAM_var = st.session_state['STREAM']

            if 'SUB_STREAM' not in st.session_state or st.session_state.df.empty:
                SUB_STREAM_var = None
            else:
                SUB_STREAM_var = st.session_state['SUB_STREAM']


            if 'TARGET_ACTION' not in st.session_state or st.session_state.df.empty:
                TARGET_ACTION_var = None
            else:
                TARGET_ACTION_var = st.session_state['TARGET_ACTION']

            if 'CHANNEL' not in st.session_state or st.session_state.df.empty:
                CHANNEL_var = None
            else:
                CHANNEL_var = st.session_state['CHANNEL']

            if 'CAMPAIGN_TYPE' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_TYPE_var = None
            else:
                CAMPAIGN_TYPE_var = st.session_state['CAMPAIGN_TYPE']

            if 'CAMPAIGN_NAME' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_NAME_var = None
            else:
                CAMPAIGN_NAME_var = st.session_state['CAMPAIGN_NAME']

            if 'CAMPAIGN_DESC' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_DESC_var = None
            else:
                CAMPAIGN_DESC_var = st.session_state['CAMPAIGN_DESC']

            if 'CAMPAIGN_TEXT' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_TEXT_var = None
            else:
                CAMPAIGN_TEXT_var = st.session_state['CAMPAIGN_TEXT']

            if 'CAMPAIGN_MODEL' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_MODEL_var = None
            else:
                CAMPAIGN_MODEL_var = st.session_state['CAMPAIGN_MODEL']

            if 'CDS_LAUNCHER' not in st.session_state or st.session_state.df.empty:
                CDS_LAUNCHER_var = None
            else:
                CDS_LAUNCHER_var = st.session_state['CDS_LAUNCHER']

            if 'CAMPAIGN_TEXT_KZ' not in st.session_state or st.session_state.df.empty:
                CAMPAIGN_TEXT_KZ_var = None
            else:
                CAMPAIGN_TEXT_KZ_var = st.session_state['CAMPAIGN_TEXT_KZ']
            if 'CAMPAIGNCODE' not in st.session_state or st.session_state.df.empty:
                CAMPAIGNCODE_var = None
            else:
                CAMPAIGNCODE_var = st.session_state['CAMPAIGNCODE']

            if 'RECORD' not in st.session_state:
                st.session_state['RECORD'] = None

           
            if st.button('Существующая кампания'):
                if campaign_id:
                    with get_connection_DSSB_OCDS() as conn:
                        campaign_data = fetch_campaign_data_by_id_main(conn, campaign_id, 'dssb_ocds.mb01_camp_dict')
                        if campaign_data:
                            st.success("Campaign ID found. Filling up the rest of the fields...")

                            st.session_state['STREAM'] = campaign_data.get('STREAM')
                            st.session_state['SUB_STREAM'] = campaign_data.get('SUB_STREAM')
                            st.session_state['TARGET_ACTION'] = campaign_data.get('TARGET_ACTION')
                            st.session_state['CHANNEL'] = campaign_data.get('CHANNEL')
                            st.session_state['CAMPAIGN_TYPE'] = campaign_data.get('CAMPAIGN_TYPE')
                            st.session_state['CAMPAIGN_NAME'] = campaign_data.get('CAMPAIGN_NAME')
                            st.session_state['CAMPAIGN_DESC'] = campaign_data.get('CAMPAIGN_DESC')
                            st.session_state['CAMPAIGN_TEXT'] = campaign_data.get('CAMPAIGN_TEXT')
                            st.session_state['CAMPAIGN_MODEL'] = campaign_data.get('CAMPAIGN_MODEL')
                            st.session_state['CDS_LAUNCHER'] = campaign_data.get('CDS_LAUNCHER')
                            st.session_state['CAMPAIGN_TEXT_KZ'] = campaign_data.get('CAMPAIGN_TEXT_KZ')
                            st.session_state['CAMPAIGNCODE'] = campaign_data.get('CAMPAIGNCODE')
                           

                           

                            STREAM_var = campaign_data.get('STREAM')
                            SUB_STREAM_var = campaign_data.get('SUB_STREAM')
                            TARGET_ACTION_var = campaign_data.get('TARGET_ACTION')
                            CHANNEL_var = campaign_data.get('CHANNEL')
                            CAMPAIGN_TYPE_var = campaign_data.get('CAMPAIGN_TYPE')
                            CAMPAIGN_NAME_var = campaign_data.get('CAMPAIGN_NAME')
                            CAMPAIGN_DESC_var = campaign_data.get('CAMPAIGN_DESC')
                            CAMPAIGN_TEXT_var = campaign_data.get('CAMPAIGN_TEXT')
                            CAMPAIGN_MODEL_var = campaign_data.get('CAMPAIGN_MODEL')
                            CDS_LAUNCHER_var = campaign_data.get('CDS_LAUNCHER')
                            CAMPAIGN_TEXT_KZ_var = campaign_data.get('CAMPAIGN_TEXT_KZ')
                            CAMPAIGNCODE_var = campaign_data.get('CAMPAIGNCODE')
                       
           
                        else:
                            st.warning("Campaign ID not found. Please enter the details manually.")
                else:
                    st.error("Please enter a Campaign ID to check.")

            if st.button('Новая компания'):
                # Start of Streamlit UI
                if 'df' not in st.session_state or st.session_state.df.empty:
                    st.warning("Данные не загружены. Пожалуйста выберите данные к загрузке")
                else:
                    table_name = 'msbmckinsey.URK_00035232_CDS_MSB'
                    with get_connection_DSSB_OCDS() as conn:
                        campaign_data = fetch_campaign_data_by_id(conn, 'dssb_ocds.mb01_camp_dict')
                        if campaign_data:
                            st.session_state['CAMPAIGNCODE'] = campaign_data.get('CAMPAIGNCODE')
                            CAMPAIGNCODE_old = campaign_data.get('CAMPAIGNCODE')
                            CAMPAIGNCODE_var = increment_string(CAMPAIGNCODE_old)
                            st.session_state.camp = CAMPAIGNCODE_var
                            st.session_state.saved_data = 0

            if 'CAMPAIGNCODE' not in st.session_state:
                st.warning('Выберите CAMPAIGNCODE')
            else:
                st.success(f'Новый CAMPAIGNCODE: {st.session_state.camp}')


            if CAMPAIGNCODE_var is not None:

                if stratify_check:

                    stratification_path = st.selectbox("Stratification?:", ["Yes", "No"],  index = 1)
   
                    if stratification_path == 'No':
                        st.info('Не используем стратификационную выборку')
                    if stratification_path == 'Yes':
                        data_file = st.file_uploader("Загрузите файл данных (Parquet)", type=['parquet'])
                        if data_file is not None:
                            st.session_state.df = pd.read_parquet(data_file)
                            st.write(st.session_state.df.head(500))
                            num_rows = st.session_state.df.shape[0]
                            st.success("Данные стратификации успешно загружены и обновлены.")
                            st.success(f"В страте {num_rows} записей")


                col7, col8 = st.columns(2)
                       
                with col7:
                    channel_path = st.radio("Выберите channel:",('Push', 'POP-UP', 'СМС'))

                with col8:
                    is_bonus = st.checkbox('Бонусная кампания?')
                    if is_bonus:
                        if st.button('Новый код XLS_OW_ID'):
                            with get_connection_DSSB_OCDS() as conn:
                                campaign_data = fetch_campaign_data_by_XLS(conn)
                                if campaign_data:
                                    st.session_state['XLS_OW_ID'] = campaign_data.get('XLS_OW_ID')
                                    CAMPAIGNCODE_old = campaign_data.get('XLS_OW_ID')
                                    CAMPAIGNCODE_var = increment_string_XLS(CAMPAIGNCODE_old)
                                    st.success(f'Новый XLS_OW_ID: {CAMPAIGNCODE_var}')

                RB_channel_path = st.radio("Выберите channel:",('RB1', 'RB3'))

               
                table_name_target = 'dssb_ocds.mb22_local_target'
                table_name_control = 'dssb_ocds.mb21_local_control'
                if channel_path == 'Push':
                    table_name_target_main = 'fd_rb2_campaigns_users'

                    table_name_off_limit = 'off_limit_campaigns_users'
                    st.success(f'грузим в {table_name_target_main}')
                    st.success(f'грузим в {table_name_off_limit}')
                if channel_path == 'POP-UP':
                    table_name_target_main = 'FD_RB2_POP_UP_CAMPAIGN'

                    table_name_off_limit = 'off_limit_campaigns_users'
                    st.success(f'грузим в {table_name_target_main}')
                   
                if channel_path == 'СМС':

                    table_name_target_main = 'fd_rb2_campaigns_users'

                    table_name_off_limit = 'off_limit_campaigns_users'
                    st.success(f'грузим в {table_name_target_main}')
                    st.success(f'грузим в {table_name_off_limit}')

                    if st.button('Добавить телефон'):
                        with get_connection_DSSB_OCDS() as conn:
                            phones = fetch_phones(conn)
                            st.session_state.df = pd.merge(st.session_state.df, phones, on = 'IIN', how = 'inner')

                if st.session_state.saved_data == 0:
                   
                    if RB_channel_path == 'RB1':
                        # Collecting campaign-related information
                        CAMPAIGNCODE = st.text_input("CAMPAIGNCODE:")
                        #CAMPAIGNCODE = st.text_input("Campaign ID:")
                        DATE_START = st.date_input("Start Date:")
                        DATE_END = st.date_input("End Date:")
                        STREAM = st.text_input("STREAM::", STREAM_var)
                        SUB_STREAM = st.text_input("SUB_STREAM:", SUB_STREAM_var)
                        TARGET_ACTION = st.text_input("TARGET_ACTION:", TARGET_ACTION_var)
                        CHANNEL = st.text_input("CHANNEL:", CHANNEL_var)
                        CAMPAIGN_TYPE = st.text_input("CAMPAIGN_TYPE:", CAMPAIGN_TYPE_var)
                        CAMPAIGN_NAME = st.text_input("CAMPAIGN_NAME:", CAMPAIGN_NAME_var)
                        CAMPAIGN_DESC = st.text_input("CAMPAIGN_DESC:", CAMPAIGN_DESC_var)
                        CAMPAIGN_TEXT = st.text_input("CAMPAIGN_TEXT:", CAMPAIGN_TEXT_var)
                        CAMPAIGN_MODEL = st.text_input("CAMPAIGN_MODEL:", CAMPAIGN_MODEL_var)
                        CDS_LAUNCHER = st.text_input("CDS_LAUNCHER:", CDS_LAUNCHER_var)
                        CAMPAIGN_TEXT_KZ = st.text_input("CAMPAIGN_TEXT_KZ:", CAMPAIGN_TEXT_KZ_var)
                        SHORT_DESC = st.text_input("SHORT_DESC:")

                        OUT_DATE = st.date_input("дата отправки, она должна быть минимум на следующий день:")
                        CAMP_CNT = st.text_input("кол-во выборки:")
   
                    if RB_channel_path == 'RB3':
                        # Collecting campaign-related information
                        CAMPAIGNCODE = st.text_input("CAMPAIGNCODE:")
                        #CAMPAIGNCODE = st.text_input("Campaign ID:")
                        DATE_START = st.date_input("Start Date:")
                        DATE_END = st.date_input("End Date:")
                        STREAM = st.text_input("STREAM::", STREAM_var)
                        SUB_STREAM = st.text_input("SUB_STREAM:", SUB_STREAM_var)
                        TARGET_ACTION = st.text_input("TARGET_ACTION:", TARGET_ACTION_var)
                        CHANNEL = st.text_input("CHANNEL:", CHANNEL_var)
                        CAMPAIGN_TYPE = st.text_input("CAMPAIGN_TYPE:", CAMPAIGN_TYPE_var)
                        CAMPAIGN_NAME = st.text_input("CAMPAIGN_NAME:", CAMPAIGN_NAME_var)
                        CAMPAIGN_DESC = st.text_input("CAMPAIGN_DESC:", CAMPAIGN_DESC_var)
                        CAMPAIGN_TEXT = st.text_input("CAMPAIGN_TEXT:", CAMPAIGN_TEXT_var)
                        CAMPAIGN_MODEL = st.text_input("CAMPAIGN_MODEL:", CAMPAIGN_MODEL_var)
                        CDS_LAUNCHER = st.text_input("CDS_LAUNCHER:", CDS_LAUNCHER_var)
                        CAMPAIGN_TEXT_KZ = st.text_input("CAMPAIGN_TEXT_KZ:", CAMPAIGN_TEXT_KZ_var)
                        SHORT_DESC = st.text_input("SHORT_DESC:")
                        BONUS = st.text_input("BONUS:")
                        CHARACTERISTIC_JSON = st.text_input("CHARACTERISTIC_JSON:")
                        XLS_OW_ID = st.text_input("XLS_OW_ID:")

                        OUT_DATE = st.date_input("дата отправки, она должна быть минимум на следующий день:")
                        CAMP_CNT = st.text_input("кол-во выборки:")

                if st.button(f'Сохранить данные'):
                    if RB_channel_path == 'RB1':
                        fields = {
                            'DATE_START': DATE_START,
                            'DATE_END': DATE_END,
                            'STREAM': STREAM,
                            'SUB_STREAM': SUB_STREAM,
                            'TARGET_ACTION': TARGET_ACTION,
                            'CHANNEL': CHANNEL,
                            'CAMPAIGN_TYPE': CAMPAIGN_TYPE,
                            'CAMPAIGN_NAME': CAMPAIGN_NAME,
                            'CAMPAIGN_DESC': CAMPAIGN_DESC,
                            'CAMPAIGN_TEXT': CAMPAIGN_TEXT,
                            'CAMPAIGN_MODEL': CAMPAIGN_MODEL,
                            'CDS_LAUNCHER': CDS_LAUNCHER,
                            'CAMPAIGN_TEXT_KZ': CAMPAIGN_TEXT_KZ,
                            'SHORT_DESC': SHORT_DESC,
                            'OUT_DATE': OUT_DATE,
                            'CAMP_CNT': CAMP_CNT,
                        }
                    if RB_channel_path == 'RB3':
                        fields = {
                            'DATE_START': DATE_START,
                            'DATE_END': DATE_END,
                            'STREAM': STREAM,
                            'SUB_STREAM': SUB_STREAM,
                            'TARGET_ACTION': TARGET_ACTION,
                            'CHANNEL': CHANNEL,
                            'CAMPAIGN_TYPE': CAMPAIGN_TYPE,
                            'CAMPAIGN_NAME': CAMPAIGN_NAME,
                            'CAMPAIGN_DESC': CAMPAIGN_DESC,
                            'CAMPAIGN_TEXT': CAMPAIGN_TEXT,
                            'CAMPAIGN_MODEL': CAMPAIGN_MODEL,
                            'CDS_LAUNCHER': CDS_LAUNCHER,
                            'CAMPAIGN_TEXT_KZ': CAMPAIGN_TEXT_KZ,
                            'SHORT_DESC': SHORT_DESC,
                            'BONUS': BONUS,
                            'CHARACTERISTIC_JSON': CHARACTERISTIC_JSON,
                            'XLS_OW_ID': XLS_OW_ID,
                            'OUT_DATE': OUT_DATE,
                            'CAMP_CNT': CAMP_CNT,
                        }
                    # Set session state for each field
                    for key, value in fields.items():
                        st.session_state[key] = value if value else None
                   
                    # For CAMPAIGNCODE, if you need it separately
                    if CAMPAIGNCODE:
                        st.session_state.CAMPAIGNCODE = CAMPAIGNCODE
                    else:
                        st.session_state.CAMPAIGNCODE = None
                   
                    st.session_state.saved_data = 1
                    st.rerun()

                if  st.session_state.saved_data == 1:
                    if RB_channel_path == 'RB1':
                        st.info(f'CAMPAIGNCODE: {st.session_state.CAMPAIGNCODE}')
                        st.info(f'DATE_START: {st.session_state.DATE_START}')
                        st.info(f'DATE_END: {st.session_state.DATE_END}')
                        st.info(f'STREAM: {st.session_state.STREAM}')
                        st.info(f'SUB_STREAM: {st.session_state.SUB_STREAM}')
                        st.info(f'TARGET_ACTION: {st.session_state.TARGET_ACTION}')
                        st.info(f'CHANNEL: {st.session_state.CHANNEL}')
                        st.info(f'CAMPAIGN_TYPE: {st.session_state.CAMPAIGN_TYPE}')
                        st.info(f'CAMPAIGN_NAME: {st.session_state.CAMPAIGN_NAME}')
                        st.info(f'CAMPAIGN_DESC: {st.session_state.CAMPAIGN_DESC}')
                        st.info(f'CAMPAIGN_TEXT: {st.session_state.CAMPAIGN_TEXT}')
                        st.info(f'CAMPAIGN_MODEL: {st.session_state.CAMPAIGN_MODEL}')
                        st.info(f'CDS_LAUNCHER: {st.session_state.CDS_LAUNCHER}')
                        st.info(f'CAMPAIGN_TEXT_KZ: {st.session_state.CAMPAIGN_TEXT_KZ}')
                        st.info(f'SHORT_DESC: {st.session_state.SHORT_DESC}')
                        st.info(f'OUT_DATE: {st.session_state.OUT_DATE}')
                        st.info(f'CAMP_CNT: {st.session_state.CAMP_CNT}')

                    if RB_channel_path == 'RB3':
                        st.info(f'CAMPAIGNCODE: {st.session_state.CAMPAIGNCODE}')
                        st.info(f'DATE_START: {st.session_state.DATE_START}')
                        st.info(f'DATE_END: {st.session_state.DATE_END}')
                        st.info(f'STREAM: {st.session_state.STREAM}')
                        st.info(f'SUB_STREAM: {st.session_state.SUB_STREAM}')
                        st.info(f'TARGET_ACTION: {st.session_state.TARGET_ACTION}')
                        st.info(f'CHANNEL: {st.session_state.CHANNEL}')
                        st.info(f'CAMPAIGN_TYPE: {st.session_state.CAMPAIGN_TYPE}')
                        st.info(f'CAMPAIGN_NAME: {st.session_state.CAMPAIGN_NAME}')
                        st.info(f'CAMPAIGN_DESC: {st.session_state.CAMPAIGN_DESC}')
                        st.info(f'CAMPAIGN_TEXT: {st.session_state.CAMPAIGN_TEXT}')
                        st.info(f'CAMPAIGN_MODEL: {st.session_state.CAMPAIGN_MODEL}')
                        st.info(f'CDS_LAUNCHER: {st.session_state.CDS_LAUNCHER}')
                        st.info(f'CAMPAIGN_TEXT_KZ: {st.session_state.CAMPAIGN_TEXT_KZ}')
                        st.info(f'SHORT_DESC: {st.session_state.SHORT_DESC}')
                        st.info(f'BONUS: {st.session_state.BONUS}')
                        st.info(f'CHARACTERISTIC_JSON: {st.session_state.CHARACTERISTIC_JSON}')
                        st.info(f'XLS_OW_ID: {st.session_state.XLS_OW_ID}')
                        st.info(f'OUT_DATE: {st.session_state.OUT_DATE}')
                        st.info(f'CAMP_CNT: {st.session_state.CAMP_CNT}')

                    if RB_channel_path == 'RB1':

                        additional_info_two = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'DATE_START': st.session_state.DATE_START,
                            'DATE_END': st.session_state.DATE_END,
                            'STREAM': st.session_state.STREAM,
                            'INSET_DATETIME': now,
                        }
       
                        additional_info_three = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'UPLOAD_DATE': st.session_state.DATE_START,
                            'SHORT_DESC': st.session_state.SHORT_DESC,
                        }
                   
                        additional_info = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            #'DATE_START': st.session_state.DATE_START,
                            #'DATE_END': st.session_state.DATE_END,
                            'STREAM': st.session_state.STREAM,
                            'INSERT_DATETIME': now,
                            'SUB_STREAM': st.session_state.SUB_STREAM,
                            'TARGET_ACTION': st.session_state.TARGET_ACTION,
                            'CHANNEL': st.session_state.CHANNEL ,
                            'CAMPAIGN_TYPE': st.session_state.CAMPAIGN_TYPE,
                            'CAMPAIGN_NAME': st.session_state.CAMPAIGN_NAME,
                            'CAMPAIGN_DESC': st.session_state.CAMPAIGN_DESC,
                            'CAMPAIGN_TEXT': st.session_state.CAMPAIGN_TEXT,
                            'CAMPAIGN_MODEL': st.session_state.CAMPAIGN_MODEL,
                            'CDS_LAUNCHER': st.session_state.CDS_LAUNCHER,
                            'CAMPAIGN_TEXT_KZ': st.session_state.CAMPAIGN_TEXT_KZ,
                            'OUT_DATE': st.session_state.OUT_DATE,
                            'CAMP_CNT': st.session_state.CAMP_CNT,
                        }

                    if RB_channel_path == 'RB3':

                        additional_info_rb3 = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'DATE_START': st.session_state.DATE_START,
                            'DATE_END': st.session_state.DATE_END,
                            'XLS_OW_ID': st.session_state.XLS_OW_ID,
                            'TARGET_ACTION': st.session_state.TARGET_ACTION,
                            'BONUS': st.session_state.BONUS,
                            'CHARACTERISTIC_JSON': st.session_state.CHARACTERISTIC_JSON,
                        }
   
                        additional_info_three = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'UPLOAD_DATE': st.session_state.DATE_START,
                            'SHORT_DESC': st.session_state.SHORT_DESC,
                        }

                        additional_info_two = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'DATE_START': st.session_state.DATE_START,
                            'DATE_END': st.session_state.DATE_END,
                            'STREAM': st.session_state.STREAM,
                            'INSET_DATETIME': now,
                        }
       
                        additional_info_three = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            'UPLOAD_DATE': st.session_state.DATE_START,
                            'SHORT_DESC': st.session_state.SHORT_DESC,
                        }
                   
                        additional_info = {
                            'CAMPAIGNCODE': st.session_state.CAMPAIGNCODE,
                            #'DATE_START': st.session_state.DATE_START,
                            #'DATE_END': st.session_state.DATE_END,
                            'STREAM': st.session_state.STREAM,
                            'INSERT_DATETIME': now,
                            'SUB_STREAM': st.session_state.SUB_STREAM,
                            'TARGET_ACTION': st.session_state.TARGET_ACTION,
                            'CHANNEL': st.session_state.CHANNEL ,
                            'CAMPAIGN_TYPE': st.session_state.CAMPAIGN_TYPE,
                            'CAMPAIGN_NAME': st.session_state.CAMPAIGN_NAME,
                            'CAMPAIGN_DESC': st.session_state.CAMPAIGN_DESC,
                            'CAMPAIGN_TEXT': st.session_state.CAMPAIGN_TEXT,
                            'CAMPAIGN_MODEL': st.session_state.CAMPAIGN_MODEL,
                            'CDS_LAUNCHER': st.session_state.CDS_LAUNCHER,
                            'CAMPAIGN_TEXT_KZ': st.session_state.CAMPAIGN_TEXT_KZ,
                            'OUT_DATE': st.session_state.OUT_DATE,
                            'CAMP_CNT': st.session_state.CAMP_CNT,


                           
                        }


                if  st.session_state.saved_data == 1:
                    with st.expander("Добавить ИИН"):
                        extra_iin = st.text_input("IIN:")
                        if len(str(extra_iin)) != 12:
                            st.error('Please enter correct IIN')
                        else:
                            df_one_line = st.session_state.df.iloc[[0]]
                            df_one_line.at[0, 'IIN'] = extra_iin
                           
                            if st.button(f'Добавить ИИН в {table_name_target} и {table_name_target_main}'):
                                if st.session_state.CAMPAIGNCODE:  # Check mandatory fields
                                    column_mapping = {
                                        'IIN': 'IIN',
                                        'IIN': 'P_SID',
                                    }
                                   
                                    # Add IIN to mb22_local_target
                                    conn_dssb = get_connection_DSSB_OCDS()
                                    insert_dataframe_to_oracle(df_one_line, conn_dssb, table_name_target, column_mapping, additional_info_two)
                                    st.success("ИИН успешно добавлен в mb22_local_target")
                                   
                                    # Add IIN to fd_rb2_campaigns_users
                                    conn_spss = get_connection_SPSS_OCDS()
                                    insert_dataframe_to_oracle(df_one_line, conn_spss, table_name_target_main, column_mapping, additional_info_three)
                                    st.success("ИИН успешно добавлен в fd_rb2_campaigns_users")
                                   
                                    # Log the addition of IIN
                                    logging.info(f"IIN {extra_iin} added to both tables - User: {st.session_state.get('username', 'Unknown')}, Campaign Code: {st.session_state.CAMPAIGNCODE}")
                                else:
                                    st.error("Пожалуйста заполните все необходимые поля")
                                    logging.warning(f"Failed to add IIN {extra_iin} - Missing fields - User: {st.session_state.get('username', 'Unknown')}, Campaign Code: {st.session_state.CAMPAIGNCODE}")
           
   
                    col10, col11 = st.columns(2)
                    # Separate buttons in an expander
   
                    # Display upload readiness status
                    st.subheader("Проверка готовности к загрузке")
                    display_upload_status()
                    
                    # Skip mode toggle
                    skip_mode = st.checkbox(
                        "🔄 **Режим пропуска проблемных записей**", 
                        value=True, 
                        help="Включите для автоматического пропуска дубликатов и проблемных записей. Выключите для остановки при первой ошибке."
                    )
                    
                    if skip_mode:
                        st.info("💡 **Режим пропуска активен**: Дубликаты и проблемные записи будут пропущены, остальные данные загрузятся успешно.")
                    else:
                        st.warning("⚠️ **Строгий режим**: Загрузка остановится при первой ошибке.")
                    
                    st.markdown("---")
   
                    num_rows = st.session_state.df.shape[0]

                    with col11:
                        # Button for mb01_camp_dict
                        if st.button(f'Загрузить данные в mb01_camp_dict'):
                            table_name = 'dssb_ocds.mb01_camp_dict'
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_message = (f"User: {st.session_state['username']}, "
                                           f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                           f"Button Press Time: {current_time}, "
                                           f"STREAM: {st.session_state.STREAM}, "
                                           f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                           f"Action: Upload to mb01_camp_dict")
                            logging.info(log_message)
                   
                            # Pre-validation
                            if not st.session_state.CAMPAIGNCODE:
                                st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                            elif not st.session_state.STREAM:
                                st.error("❌ Ошибка валидации: STREAM не заполнен. Пожалуйста заполните поток кампании.")
                                logging.warning(f"Validation failed - Missing STREAM - {log_message}")
                            else:
                                # Perform upload with improved error handling
                                column_mapping = {
                                    #'IIN': 'IIN',
                                    #'P_SID': 'P_SID',
                                }
                                df_one_line = st.session_state.df.iloc[[0]]
                                
                                success = safe_database_upload(
                                    df_one_line, 
                                    get_connection_DSSB_OCDS, 
                                    table_name, 
                                    column_mapping, 
                                    additional_info,
                                    f"Upload to {table_name}"
                                )
                                
                            if success:
                                logging.info(f"Successful upload to mb01_camp_dict - {log_message}")
                                #s = ['NadirB@halykbank.kz', 'MadiB@halykbank.kz', 'BibarysM@halykbank.kz', 'DaniyarMussa@halykbank.kz', 'AlfiyaEl@halykbank.kz']
                                s = ['NadirB@halykbank.kz']
                                email_message = f"Successful upload to mb01_camp_dict - {log_message}"
                                send_email(s, 'Automated campaign', email_message)
                            else:
                                logging.error(f"Failed upload to mb01_camp_dict - {log_message}")
   
                        st.write('')
   
                    with col10:
   
                        st.write('Таблицы РБ1:')
                        st.write('---------------------------------------------------------------------------------------------------------------------------------------------')
   
   
   
                    with col11:
   
   
                        # Button for rb3_tr_campaign_dict
                        if st.button(f'Загрузить данные в rb3_tr_campaign_dict'):
                            table_name = 'dssb_ocds.rb3_tr_campaign_dict'
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_message = (f"User: {st.session_state['username']}, "
                                           f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                           f"Button Press Time: {current_time}, "
                                           f"STREAM: {st.session_state.STREAM}, "
                                           f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                           f"Action: Upload to rb3_tr_campaign_dict")
                            logging.info(log_message)
                   
                            # Pre-validation
                            if not st.session_state.CAMPAIGNCODE:
                                st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                            elif not st.session_state.STREAM:
                                st.error("❌ Ошибка валидации: STREAM не заполнен. Пожалуйста заполните поток кампании.")
                                logging.warning(f"Validation failed - Missing STREAM - {log_message}")
                            else:
                                # Perform upload with improved error handling
                                column_mapping = {
                                    #'IIN': 'IIN',
                                    #'P_SID': 'P_SID',
                                }
                                df_one_line = st.session_state.df.iloc[[0]]
                                
                                success = safe_database_upload(
                                    df_one_line, 
                                    get_connection_DSSB_OCDS, 
                                    table_name, 
                                    column_mapping, 
                                    additional_info_rb3,
                                    f"Upload to {table_name}"
                                )
                                
                            if success:
                                logging.info(f"Successful upload to rb3_tr_campaign_dict - {log_message}")
                                #s = ['NadirB@halykbank.kz', 'MadiB@halykbank.kz', 'BibarysM@halykbank.kz', 'DaniyarMussa@halykbank.kz', 'AlfiyaEl@halykbank.kz']
                                s = ['NadirB@halykbank.kz']
                                email_message = f"Successful upload to rb3_tr_campaign_dict - {log_message}"
                                send_email(s, 'Automated campaign', email_message)
                            else:
                                    logging.error(f"Failed upload to rb3_tr_campaign_dict - {log_message}")
       
       
                        # Button for off_limit_campaigns_users
                        if st.button(f'Загрузить данные в {table_name_off_limit}'):
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_message = (f"User: {st.session_state['username']}, "
                                           f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                           f"Button Press Time: {current_time}, "
                                           f"STREAM: {st.session_state.STREAM}, "
                                           f"ROW NUM: {num_rows}, "
                                           f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                           f"Action: Upload to off_limit_campaigns_users")
                            logging.info(log_message)
                   
                            # Pre-validation
                            if not st.session_state.CAMPAIGNCODE:
                                st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                            else:
                                # Prepare column mapping based on channel
                                if channel_path != 'СМС':
                                    column_mapping = {
                                        'IIN': 'IIN',
                                        'P_SID': 'P_SID',
                                    }
                                else:
                                    column_mapping = {
                                        'IIN': 'IIN',
                                        'P_SID': 'P_SID',
                                        'PHONE': 'PHONE',
                                    }
                                    # Check if phone column exists for SMS campaigns
                                    if 'PHONE' not in st.session_state.df.columns:
                                        st.error("❌ Ошибка данных: Для СМС кампаний требуется колонка PHONE. Нажмите 'Добавить телефон' сначала.")
                                        logging.error(f"Missing PHONE column for SMS campaign - {log_message}")
                                        return
                                
                                                                # Perform upload with optional skip functionality  
                                if skip_mode:
                                    success, failed_records = safe_database_upload_with_skip(
                                        st.session_state.df, 
                                        get_connection_SPSS_OCDS, 
                                        table_name_off_limit, 
                                        column_mapping, 
                                        additional_info_three,
                                        f"Upload to {table_name_off_limit}",
                                        allow_skip=True
                                    )
                                else:
                                    success = safe_database_upload(
                                        st.session_state.df, 
                                        get_connection_SPSS_OCDS, 
                                        table_name_off_limit, 
                                        column_mapping, 
                                        additional_info_three,
                                        f"Upload to {table_name_off_limit}"
                                    )
                                    failed_records = []
                                
                                if success:
                                    if failed_records:
                                        logging.info(f"Partial upload to off_limit_campaigns_users - {log_message} - {len(failed_records)} records skipped")
                                        st.info(f"✅ Загрузка завершена с пропуском {len(failed_records)} дубликатов")
                                    else:
                                        logging.info(f"Successful upload to off_limit_campaigns_users - {log_message}")
                                else:
                                    logging.error(f"Failed upload to off_limit_campaigns_users - {log_message}")
   
                        st.write('')
   
                    with col10:
   
                        st.write('Таблицы РБ3:')
                        st.write('---------------------------------------------------------------------------------------------------------------------------------------------')
        
                    with col11:

                        # Button for fd_rb2_campaigns_users
                        if st.button(f'Загрузить данные в {table_name_target_main}'):
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_message = (f"User: {st.session_state['username']}, "
                                           f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                           f"Button Press Time: {current_time}, "
                                           f"STREAM: {st.session_state.STREAM}, "
                                           f"ROW NUM: {num_rows}, "
                                           f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                           f"Action: Upload to fd_rb2_campaigns_users")
                            logging.info(log_message)
                           
                            st.session_state['RECORD'] = st.session_state.CAMPAIGNCODE
                           
                            # Pre-validation
                            if not st.session_state.CAMPAIGNCODE:
                                st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                            elif not st.session_state.SHORT_DESC:
                                st.error("❌ Ошибка валидации: SHORT_DESC не заполнен. Пожалуйста заполните краткое описание кампании.")
                                logging.warning(f"Validation failed - Missing SHORT_DESC - {log_message}")
                            else:
                                # Prepare data with IIN formatting
                                df_to_load = st.session_state.df.copy()
                               
                                # Create two columns from the same IIN data but with different types
                                if 'IIN' in df_to_load.columns:
                                    # IIN_STR will be used for the IIN column (as string with leading zeros)
                                    df_to_load['IIN_STR'] = df_to_load['IIN'].astype(str).str.zfill(12)  # Ensure 12 digits with leading zeros
                                   
                                    # IIN_INT will be used for the P_SID column (as integer without leading zeros)
                                    df_to_load['IIN_INT'] = df_to_load['IIN'].astype(int)
                                   
                                    # Then update the mapping to use these new columns
                                    column_mapping = {
                                        'IIN_STR': 'IIN',  # Map string version to IIN column
                                        'IIN_INT': 'P_SID'  # Map integer version to P_SID column
                                    }
                                else:
                                    st.error("❌ Ошибка данных: В выборке не найдена колонка IIN. Проверьте структуру данных.")
                                    logging.error(f"Missing IIN column in dataframe - {log_message}")
                                    return
                               
                                                                # Perform upload with optional skip functionality
                                if skip_mode:
                                    success, failed_records = safe_database_upload_with_skip(
                                        df_to_load, 
                                        get_connection_SPSS_OCDS, 
                                        table_name_target_main, 
                                        column_mapping, 
                                        additional_info_three,
                                        f"Upload to {table_name_target_main}",
                                        allow_skip=True
                                    )
                                else:
                                    success = safe_database_upload(
                                        df_to_load, 
                                        get_connection_SPSS_OCDS, 
                                        table_name_target_main, 
                                        column_mapping, 
                                        additional_info_three,
                                        f"Upload to {table_name_target_main}"
                                    )
                                    failed_records = []
                                
                                if success:
                                    if failed_records:
                                        logging.info(f"Partial upload to fd_rb2_campaigns_users - {log_message} - {len(failed_records)} records skipped")
                                        st.info(f"✅ Кампания {st.session_state.CAMPAIGNCODE} готова к запуску! (Пропущено {len(failed_records)} дубликатов)")
                                    else:
                                        logging.info(f"Successful upload to fd_rb2_campaigns_users - {log_message}")
                                        st.info(f"✅ Кампания {st.session_state.CAMPAIGNCODE} готова к запуску!")
                                else:
                                    logging.error(f"Failed upload to fd_rb2_campaigns_users - {log_message}")
                                    st.warning("⚠️ Возможны проблемы с запуском кампании из-за ошибки загрузки.")
                   
                        # Button for mb22_local_target
                        if st.button(f'Загрузить данные в {table_name_target}'):
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_message = (f"User: {st.session_state['username']}, "
                                           f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                           f"Button Press Time: {current_time}, "
                                           f"STREAM: {st.session_state.STREAM}, "
                                           f"ROW NUM: {num_rows}, "
                                           f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                           f"Action: Upload to mb22_local_target")
                            logging.info(log_message)
                   
                            # Pre-validation
                            if not st.session_state.CAMPAIGNCODE:
                                st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                            else:
                                column_mapping = {
                                    'IIN': 'IIN',
                                    'IIN': 'P_SID',
                                }
                                
                                # Perform upload with improved error handling
                                success = safe_database_upload(
                                    st.session_state.df, 
                                    get_connection_DSSB_OCDS, 
                                    table_name_target, 
                                    column_mapping, 
                                    additional_info_two,
                                    f"Upload to {table_name_target}"
                                )
                                
                            if success:
                                logging.info(f"Successful upload to mb22_local_target - {log_message}")
                            else:
                                    logging.error(f"Failed upload to mb22_local_target - {log_message}")
   
                        st.write('')
   
                    with col10:
   
                        st.write('Таблицы общие:')
                        st.write('---------------------------------------------------------------------------------------------------------------------------------------------')
   
                    if stratify_check:
                        with col11:
                            # Button for mb22_local_control
                            if st.button(f'Загрузить данные в {table_name_control}'):
                                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                log_message = (f"User: {st.session_state['username']}, "
                                               f"Campaign Code: {st.session_state.CAMPAIGNCODE}, "
                                               f"Button Press Time: {current_time}, "
                                               f"STREAM: {st.session_state.STREAM}, "
                                               f"ROW NUM: {num_rows}, "
                                               f"SHORT_DESC: {st.session_state.SHORT_DESC}, "
                                               f"Action: Upload to mb22_local_control")
                                logging.info(log_message)
                       
                                # Pre-validation
                                if not st.session_state.CAMPAIGNCODE:
                                    st.error("❌ Ошибка валидации: CAMPAIGNCODE не заполнен. Пожалуйста заполните код кампании.")
                                    logging.warning(f"Validation failed - Missing CAMPAIGNCODE - {log_message}")
                                else:
                                    column_mapping = {
                                        'IIN': 'IIN',
                                        'IIN': 'P_SID',
                                    }
                                    
                                    # Perform upload with improved error handling
                                    success = safe_database_upload(
                                        st.session_state.df, 
                                        get_connection_DSSB_OCDS, 
                                        table_name_control, 
                                        column_mapping, 
                                        additional_info_two,
                                        f"Upload to {table_name_control}"
                                    )
                                    
                                if success:
                                    logging.info(f"Successful upload to mb22_local_control - {log_message}")
                                else:
                                        logging.error(f"Failed upload to mb22_local_control - {log_message}")
   
                            st.write('')
   
                        with col10:
   
                            st.write('Таблицы control:')
                            st.write('---------------------------------------------------------------------------------------------------------------------------------------------')
                           

               
                password_for_campaign = 'RBadmin123'
                if st.session_state['RECORD'] is not None:

                    if st.button("Сколько в базе?"):
                        code = st.session_state['RECORD']
                        st.info(f'the code is {code}')
                        with get_connection_SPSS_OCDS() as conn:
                       
                            cursor = conn.cursor()
                           
                            # Execute the query
                            query = f"SELECT COUNT(IIN) FROM {table_name_target_main} WHERE CAMPAIGNCODE = '{code}'"
   
                           
                            cursor.execute(query)
                           
                            # Fetch the result
                            result = cursor.fetchone()
                            count = result[0] if result else 0
                           
                            # Display the result
                            st.info(f"На код: '{code}'. Количество записей: {count}")

                    check_password = st.text_input("password")

                    if check_password == password_for_campaign:

                        if st.toggle('delete campaign code from db'):
                            code = st.session_state['RECORD']
                            st.write(f'Удалить кампанию: {code}?')
                            if st.button("Удалить из базы"):
                                code = st.session_state['RECORD']
                                st.info(f'Удаляем {code}')
                                with get_connection_SPSS_OCDS() as conn:
                               
                                    cursor = conn.cursor()
                                   
                                    # Execute the query
                                    query = f"DELETE FROM {table_name_target_main} WHERE CAMPAIGNCODE = '{code}'"

                                    cursor.execute(query)
                                    conn.commit()                              
                                    # Display the result
                                    st.info(f"Кампания: '{code}'. Удалена из базы {table_name_target_main} успешно.")
                           
                   
                   
       
                if st.button('Save to Excel'):
                    # Save the DataFrame to an Excel file
                    st.session_state.df.to_excel('MSB_CAMPAIGN.xlsx', index=False)
                   
                    # Open and read the file content
                    with open('MSB_CAMPAIGN.xlsx', 'rb') as file:
                        btn = st.download_button(
                            label="Download Excel",
                            data=file,
                            file_name="example_dataframe.xlsx",
                            mime="application/vnd.ms-excel"
                        )
       


   
if __name__ == "__run_main_app__":
    main()