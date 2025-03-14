# Library
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
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
    # Koneksi db internal
    mysql = 'mysql+pymysql://username:password@hostname:port/dbname'
    engine_mysql = create_engine(mysql)

    # Koneksi db erp cna
    postgre = 'postgresql://username:password@hostname:port/dbname'
    engine_postgre = create_engine(postgre)
    connect_postgre = engine_postgre.connect()

    # Akses data internal
    with engine_postgre.connect() as conn:
        access_internal = pd.read_sql_query(''' 
                                            SELECT 
                                                field_1, 
                                                field_2, 
                                                field_3 
                                            FROM your_table_name 
                                            WHERE active = TRUE
                                            AND field_1 NOT IN (your_excluded_ids)
                                            ORDER BY id ASC
                                            ''', conn)

    def extract_transform_acc_st():
        # Ekstraksi data dari tabel tertentu
        with engine_postgre.connect() as conn:
            df_access = pd.read_sql_query(''' 
                SELECT 
                    field_4, 
                    field_5, 
                    field_6, 
                    field_7 
                FROM your_table_name
                WHERE field_1 NOT IN (your_excluded_ids);
            ''', con=conn)

        # Ekstrak informasi untuk iterasi
        field_7_internal = df_access['field_7'].tolist()
        field_4_internal = df_access['field_4'].tolist()
        field_5_internal = df_access['field_5'].tolist()
        field_6_internal = df_access['field_6'].tolist()

        def extract_acc_st(field_7, field_6, mysql_conn, field_4, field_5):
            # Query untuk data yang akan diekstraksi
            with engine_mysql.connect() as conn_mysql: 
                query = text(f'''
                SELECT 
                    field_8, 
                    field_9, 
                    field_10, 
                    field_11, 
                    field_12 AS create_date, 
                    field_13 AS write_date
                FROM your_table_name
                WHERE field_14 NOT IN ('r')
                AND field_15 != ''
                AND field_16 != ''
                AND field_17 LIKE '%a%'
                AND DATE(field_11) BETWEEN '2024-01-01' AND CURDATE()
                AND (field_15 = '{field_5}' OR field_16 = '{field_5}')
                GROUP BY 
                    field_8
                ORDER BY 
                    field_11 DESC, field_9;
                ''')
                try:
                    df = pd.read_sql_query(query, conn_mysql, params={'field_6': field_6, 'field_5': field_5})
                except Exception as e:
                    print(f"Error saat menjalankan query MySQL: {e}")

            # Transformasi data
            df['field_6'] = field_6
            df['field_unq_trans_comp'] = df['field_8'] + '_' + df['field_6'].astype(str) + field_5
            df['coa_filter'] = field_5
            df['field_18'] = field_4

            df = df[['field_6', 'field_8', 'field_9', 'field_10', 'field_11', 'create_date', 'write_date', 'field_unq_trans_comp', 'coa_filter', 'field_18']]
            df = df.replace({np.nan: None})
            return df

        # Penggabungan data dari setiap perusahaan
        df_final = pd.DataFrame()
        for field_7, field_6, field_4, field_5 in zip(field_7_internal, field_6_internal, field_4_internal, field_5_internal):
            df_temp = extract_acc_st(field_7, field_6, field_7_internal, field_4, field_5)
            df_final = pd.concat([df_final, df_temp], ignore_index=True)

        return df_final

    # Panggil fungsi enktrak data 
    df = extract_transform_acc_st()

    # Ekstrak data dari tabel WHCB
    with engine_postgre.connect() as conn_whcb:
        df_cna = pd.read_sql_query(''' 
                                            SELECT 
                                                field_19, 
                                                field_20, 
                                                field_6, 
                                                field_11, 
                                                field_21 AS field_18, 
                                                field_unq_trans_comp, 
                                                write_date
                                            FROM your_table_name
                                            WHERE field_6 NOT IN (your_excluded_ids)
                                            AND DATE(field_11) BETWEEN '2024-01-01' AND CURRENT_DATE
                                            ORDER BY field_11;
                                            ''', conn_whcb)

    # Selisih data antara internal dan WHCB
    data_delete = df_cna[~df_cna['field_unq_trans_comp'].isin(df['field_unq_trans_comp'])]

    # Hapus data
    list_del = tuple(data_delete['field_unq_trans_comp'])
    if len(list_del) == 0:
        print('None deleted')
    else:
        print(f'Delete {len(list_del)} rows')
        with engine_postgre.connect() as conn_delete:
            if len(list_del) == 1:
                query = f'''
                DELETE FROM your_table_name
                WHERE field_unq_trans_comp = '{list_del[0]}'
                '''
            else:
                query = f'''
                DELETE FROM your_table_name
                WHERE field_unq_trans_comp IN {list_del}
                '''
            conn_delete.execute(query)

    send_chat_notification("{} - Data delete successfully from WHCB Account Statement, amount of data = {}".format(
        dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(list_del)
    ))

except Exception as e:
    error_message = f"❌ Delete data WHCB Account Statement gagal: {str(e)}"
    send_chat_notification(error_message)
    print(error_message)
