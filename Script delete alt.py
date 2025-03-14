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

# Google Chat Notification
WEBHOOK_URL = "#yourwebhookurl"

def send_chat_notification(message):
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    data = json.dumps({"text": message})
    response = requests.post(WEBHOOK_URL, headers=headers, data=data)
    if response.status_code != 200:
        print(f"Error sending notification: {response.text}")

try: 
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

    def extract_access():
        try:
            conn = get_postgres_connection()
            query = text("""
                SELECT
                    field_1,
                    field_2,
                    field_3
                FROM your_table_name
                WHERE active = TRUE AND
                    field_1 NOT IN (excluded_ids) 
                ORDER BY id ASC
            """)
            suffix_access_internal = pd.read_sql_query(query, conn)
            conn.close()

            return suffix_access_internal
        except Exception as e:
            print(f"Error Saat Mengambil Data: {e}")

    access_data = extract_access()
    print(access_data)

    def extract_source_data():
        access_internal = extract_access()

        field_2_int = access_internal['field_2'].tolist()
        field_1_int = access_internal['field_1'].tolist()
        mysql_conn_int = len(field_2_int) * ['internal_zains_db']

        def extract_zains(field_2,mysql_conn,field_1):
            conn_mysql = get_mysql_connection()
            query = f"""
                SELECT field_4
                FROM
                    zains_{field_2}.your_table_name
                WHERE DATE(field_5) >= '2024-01-01' AND CURDATE()
            """
            try:
                df = pd.read_sql_query(query, conn_mysql, params={'field_1': field_1})
            except Exception as e:
                print(f"Error saat menjalankan query MySQL: {e}")

            df['field_1'] = field_1

            conn_mysql.close()
            
            return df[['field_1','field_4']].values.tolist()
        
        source_ids = []
        for i, j, k in zip(field_2_int, mysql_conn_int,field_1_int):
            source_ids.extend(extract_zains(i, j, k))
        
        return source_ids

    source_data = extract_source_data()
    print("Source Data:", source_data)

    def extract_jr_st():
        access_internal = extract_access()
        field_1_int = access_internal['field_1'].tolist()

        def extract_whcb(field_1):
            conn_pg = get_postgres_connection()
            try:
                query = text("""
                    SELECT field_1, field_4
                    FROM your_table_name
                    WHERE 
                        field_1 = :field_1 AND
                        DATE(field_5) >= '2024-01-01'
                """)
                df = pd.read_sql_query(query, conn_pg, params={'field_1': field_1})
                return df[['field_1', 'field_4']].values.tolist()
            except Exception as e:
                print(f"Error saat menjalankan query PostgreSQL untuk field_1 {field_1}: {e}")
                return []
            finally:
                conn_pg.close()  # Menutup koneksi, baik terjadi error maupun tidak
        
        destination_ids = []
        for i in field_1_int:
            destination_ids.extend(extract_whcb(i))
        return destination_ids

    destination_data = extract_jr_st()
    print("Destination Data:", destination_data)

    def compare_delete():
        try:
            # Ambil data dari source dan destination
            source_ids = extract_source_data()
            destination_ids = extract_jr_st()

            # Konversi ke set untuk perbandingan
            source_set = set(tuple(item) for item in source_ids)
            destination_set = set(tuple(item) for item in destination_ids)

            # Data yang perlu dihapus
            data_to_delete = destination_set - source_set

            # Debugging: tampilkan data yang akan dihapus
            print(f"Data to Delete: {list(data_to_delete)}")  

            # Jika tidak ada data untuk dihapus
            if len(data_to_delete) == 0:
                print("Tidak ada data yang perlu dihapus.")
                return "no delete"

            # Jalankan query penghapusan
            deleted_records = []
            with get_postgres_connection() as conn_delete:
                for field_1, field_4 in data_to_delete:
                    query = text("""
                        DELETE FROM your_table_name
                        WHERE field_1 = :field_1 AND field_4 = :field_4
                    """)
                    conn_delete.execute(query, {'field_1': field_1, 'field_4': field_4})
                    print(f"Data Dihapus: field_1={field_1}, field_4={field_4}")
                    deleted_records.append((field_1, field_4))

            print(f"Total Data Dihapus: {len(deleted_records)}")
            return deleted_records

        except Exception as e:
            print(f"Error di compare_delete: {e}")
            return None
    deleted_data = compare_delete()
    print("Data yang Dihapus:", deleted_data)

    send_chat_notification("{} - Data delete successfully from WHCB Journal Statement, amount of data = {}".format(
        dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(deleted_data)
    ))

except Exception as e:
    error_message = f"❌ Delete data WHCB Journal Statement gagal: {str(e)}"
    send_chat_notification(error_message)
    print(error_message)
