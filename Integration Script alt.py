import pandas as pd
from sqlalchemy import create_engine, text 
from datetime import datetime, timedelta
import os 
import numpy as np
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

    def extract_suffix_access():
        try:
            conn = get_postgres_connection()
            query = text("""
                SELECT
                    field_1,
                    field_2,
                    field_3
                FROM your_table_name
                WHERE
                    active = TRUE AND
                    realtime_zains = TRUE
                    and field_1 NOT IN (excluded_ids) 
                ORDER BY id ASC
            """)
            suffix_access_internal = pd.read_sql_query(query, conn)
            conn.close()

            return suffix_access_internal
        except Exception as e:
            print(f"Error Saat Mengambil Data: {e}")

    suffix_access_data = extract_suffix_access()
    print(suffix_access_data)

    def extract_transform_acc_st():
        suffix_access_internal = extract_suffix_access()

        field_2_internal = suffix_access_internal['field_2'].tolist()
        field_1_internal = suffix_access_internal['field_1'].tolist()
        mysql_conn_internal = len(field_2_internal) * ['internal_zains_db']

        conn_dest = get_postgres_connection()

        def extract_acc_st(field_2, field_1, mysql_conn):
            conn_mysql = get_mysql_connection()
            query = text(f"""
                SELECT
                    a.field_4,
                    a.field_5,
                    a.field_6,
                    b.field_7
                FROM
                    zains_{field_2}.your_table_name a
                    LEFT JOIN (
                        SELECT
                            field_4,
                            field_8
                        FROM
                            zains_{field_2}.your_other_table_name
                        GROUP BY
                            field_8
                        ORDER BY
                            field_9 DESC ) aa on a.field_4 = aa.field_4
                    LEFT JOIN your_table_name b on aa.field_8 = b.field_8
                    WHERE
                        1
                        AND(a.field_4 LIKE '%%'
                            OR a.field_5 LIKE '%%')
                        AND a.parent = 'n'
                        AND(a.GROUP LIKE '%3%'
                            OR a.GROUP LIKE '%4%')
                    GROUP BY field_4, field_5    
                    ORDER BY
                        a.field_4,
                        a.LEVEL ASC
            """)
            try:
                df = pd.read_sql_query(query, conn_mysql, params={'field_1': field_1})
            except Exception as e:
                print(f"Error saat menjalankan query MySQL: {e}")

            # additional_field (transform)
            df['db_alias'] = field_2
            df['company_id'] = field_1
            df['coa_unq'] = df['company_id'].astype(str) + df['field_4']

            conn_mysql.close()

            return df
        
        all_data = []
        for i,j,k in zip(field_2_internal, field_1_internal, mysql_conn_internal):
            df= extract_acc_st(i,j,k)
            all_data.append(df)
        # Menggabungkan data yang diambil
        final_df = pd.concat(all_data, ignore_index=True)

        # Mengganti nilai NaN menjadi None
        final_df = final_df.replace({np.nan: None})

        conn_dest.close()

        return final_df
        
    # Panggil fungsi untuk mengekstrak dan mentransformasi data dari PostgreSQL dan MySQL
    final_df = extract_transform_acc_st()

    # Menampilkan DataFrame hasil transformasi
    print(final_df)

    def load_acc_st(final_df, batch_size=1000):
        try:
            conn_dest = get_postgres_connection()
            final_df['company_id'] = final_df['company_id'].fillna(0).astype(int)

            upsert_query = text("""
                INSERT INTO your_table_name(coa_unq, coa, nama_coa, dtu, company_id, bank, db_alias)
                VALUES(:coa_unq, :coa, :nama_coa, :dtu, :company_id, :bank, :db_alias)
                ON CONFLICT (coa_unq)
                DO UPDATE SET
                    coa_unq = excluded.coa_unq,
                    coa = excluded.coa,
                    nama_coa = excluded.nama_coa,
                    dtu = excluded.dtu,
                    company_id = excluded.company_id,
                    bank = excluded.bank,
                    db_alias = excluded.db_alias

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

    load_acc_st(final_df, batch_size=1000)

    send_chat_notification("{} - Data sent successfully to WHCB COA Active, amount of data = {}".format(
        dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(final_df)
    ))

except Exception as e:
    error_message = f"❌ Sync data WHCB COA Active gagal: {str(e)}"
    send_chat_notification(error_message)
    print(error_message)
