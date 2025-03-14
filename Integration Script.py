import pandas as pd
from sqlalchemy import create_engine, text 
from datetime import datetime, timedelta
import os 
import numpy as np
import pyarrow
import pymysql
import pg8000
import json
import requests
import datetime as dt

# Gooogle Chat Notification
WEBHOOK_URL = "#yourwebhookurl"

def send_chat_notification(message):
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    data = json.dumps({"text": message})
    response = requests.post(WEBHOOK_URL, headers=headers, data=data)
    if response.status_code != 200:
        print(f"Error sending notification: {response.text}")

try:
    # Start time
    start_time = dt.datetime.now()

    # Koneksi ke database PostgreSQL dan MySQL
    def get_postgres_connection():
        connection_string = "postgresql+pg8000://username:password@hostname:port/dbname"
        engine = create_engine(
            connection_string,
            connect_args={"timeout": 10000},
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=7200,
            pool_pre_ping=True
            )
        return engine.connect()

    def get_mysql_connection():
        connection_string = "mysql+pymysql://username:password@hostname:port/dbname?connect_timeout=10000"
        engine = create_engine(connection_string,
                            pool_size=5,
                            max_overflow=10,
                            pool_timeout=30,
                            pool_recycle=3600)
        return engine.connect()

    def extract_suffix_access():
        try:
            conn = get_postgres_connection()
            query = text("""
                SELECT  
                    field_1,
                    field_2,
                    field_3,
                    field_4
                FROM your_table_name
                WHERE field_3 NOT IN (your_excluded_ids)
            """)
            suffix_access_internal = pd.read_sql_query(query, conn)
            if suffix_access_internal.empty:
                    print("Data tidak ditemukan.")
            conn.close()

            return suffix_access_internal
        except Exception as e:
            print(f"Error Saat Mengambil Data: {e}")

    suffix_access_data = extract_suffix_access()
    print(suffix_access_data)

    # Extract data from PostgreSQL and MySQL
    def extract_transform_acc_st():
        suffix_access_internal = extract_suffix_access()

        field_4_internal = suffix_access_internal['field_4'].tolist()
        field_1_internal = suffix_access_internal['field_1'].tolist()
        field_2_internal = suffix_access_internal['field_2'].tolist()
        field_3_internal = suffix_access_internal['field_3'].tolist()
        mysql_conn_internal = len(field_4_internal) * ['internal_zains_db']

        def extract_acc_st(field_4, field_3, mysql_conn, field_1, field_2):
            query = text("""
                SELECT 
                    MAX(field_5) AS write_date
                FROM your_table_name
                WHERE field_3 = :field_3 AND field_1 = :field_1 AND field_6 BETWEEN '2024-11-01 00:00:00' AND '2024-12-31 23:59:59'
            """)
            with get_postgres_connection() as conn_dest:
                ext_wdt = pd.read_sql_query(query, conn_dest, params={'field_3': field_3, 'field_1': field_1})
                if ext_wdt.empty or ext_wdt.at[0, 'field_5'] is None:
                    ext_wdt = '2025-01-01 00:00:00'
                else:
                    ext_wdt = str(ext_wdt.at[0, 'field_5'])
            with get_mysql_connection() as conn_mysql:
                query_mysql = text("""
                    SELECT
                    field_7,
                    field_8,
                    field_9,
                    field_1,
                    field_10,
                    field_11, 
                    field_12,
                    field_13,
                    field_14,
                    field_15,
                    field_16,
                    SUM(CASE field_17 WHEN 'e' THEN field_18 ELSE 0 END) credit,
                    field_18,
                    SUM(CASE field_17 WHEN 'r' THEN field_18 ELSE 0 END) debet,
                    field_19,
                    field_20,
                    field_21,
                    IF(field_22 IS NULL, field_20, field_22) user_input,
                    field_23,
                    field_24,
                    field_25,
                    field_26,
                    field_27
                    FROM ...
                """)
                try:
                    df = pd.read_sql_query(query_mysql, conn_mysql, params={'field_3': field_3, 'field_1': field_1})
                except Exception as e:
                    print(f"Error saat menjalankan query MySQL: {e}")
            
            df['field_3'] = field_3
            df['field_28'] = df['field_23'] - pd.Timedelta(hours=7)
            df['via_bayar'] = np.where(df['field_24'] == '1', 'cash', 'bank')
            df['unq_trans_comp'] = df['field_7'] + '_' + df['field_3'].astype('str') + field_1
            df['coa_transaksi'] = df['field_10'].where(df['field_17'] == 'e', df['field_13'])
            df['nama_transaksi'] = df['field_11'].where(df['field_17'] == 'e', df['field_14'])
            df['coa_filter'] = field_1
            df['field_29'] = field_2
            df['write_date'] = df['field_27'].replace('0000-00-00 00:00:00', None)

            return df

        all_data = []
        for i, j, k, l, m in zip(field_4_internal, field_3_internal, mysql_conn_internal, field_1_internal, field_2_internal):
            df = extract_acc_st(i, j, k, l, m)
            all_data.append(df)

        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.replace({np.nan: None})

        return final_df

    final_df = extract_transform_acc_st()
    print(final_df)

    def load_data_to_postgres(final_df, batch_size=1000):
        try:
            conn_dest = get_postgres_connection()
            final_df.loc[:, 'field_30'] = final_df['field_30'].fillna(0).astype(int)
            final_df.loc[:, 'field_3'] = final_df['field_3'].fillna(0).astype(int)
            final_df.loc[:, 'field_31'] = final_df['field_31'].fillna(0).astype(int)

            upsert_query = text("""
                INSERT INTO your_table_name(field_31, field_30, field_3, field_7, field_8, field_9, field_1, field_10, field_11, field_12, field_13, field_14, field_15, field_16, field_17, field_18, field_19, field_20, field_21, field_22, field_23, field_24, field_25, field_26, field_27, field_28, field_29, field_30, field_31)
                VALUES (:field_31, :field_30, :field_3, :field_7, :field_8, :field_9, :field_1, :field_10, :field_11, :field_12, :field_13, :field_14, :field_15, :field_16, :field_17, :field_18, :field_19, :field_20, :field_21, :field_22, :field_23, :field_24, :field_25, :field_26, :field_27, :field_28, :field_29, :field_30, :field_31)
                ON CONFLICT (field_31) 
                DO UPDATE SET 
                    ...
            """)

            for start in range(0, len(final_df), batch_size):
                batch = final_df.iloc[start:start+batch_size].to_dict(orient="records")
                with conn_dest.begin() as transaction:
                    for row in batch:
                        conn_dest.execute(upsert_query, row)
                print(f"Batch {start // batch_size + 1} berhasil dimasukkan.")
            
            conn_dest.close()
            print("Semua batch berhasil dimasukkan atau diperbarui di PostgreSQL.")

        except Exception as e:
            print(f"Error saat memasukkan data ke PostgreSQL: {e}")

    load_data_to_postgres(final_df)

    send_chat_notification("{} - Data sent successfully to WHCB Account Statement (Nov-Dec 2024), amount of data = {}".format(
        dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(final_df)
    ))

except Exception as e:
    error_message = f"❌ Sync data WHCB Account Statement (Nov-Dec 2024) gagal: {str(e)}"
    send_chat_notification(error_message)
    print(error_message)
