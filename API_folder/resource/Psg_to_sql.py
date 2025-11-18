from flask_restful import Resource, Api, request
from flask import request
# from ..common.config import get_connection

import pandas as pd
import numpy as np
from flask import jsonify
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
from common.config import  get_connection, sql_get_connection, close_db


class psgr_to_sql(Resource):
    def post(self):
        data = request.get_json()
        from_db = data.get("FROM_DB") # Dev
        to_db = data.get("TO_DB")   # SQL # Cache_run - True ( psg) / False/empty - SQL
        tables = data.get("TABLES")
        # UAT SQL/ UAT DEV
        from_conn = get_connection(from_db)
        logging.info(from_conn)
        if to_db == "SQL":
            to_conn = sql_get_connection(to_db)
            logging.info(to_conn)
        else:
            to_conn = get_connection(to_db)
            logging.info(to_conn)

        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table};", from_conn)
            logging.info(df)
            if df.empty:
                logging.info(f"No data found in {table}, skipping.")
                continue

            columns = list(df.columns)
            col_str = "(" + ",".join([f"`{c}`" for c in columns]) + ")"
            placeholders = ",".join(["%s"] * len(columns))
            # "%s" is a placeholder not a datatype 
            insert_sql = f"INSERT INTO `{table}` {col_str} VALUES ({placeholders})"
            cursor = to_conn.cursor()

            for _, row in df.iterrows():
                cursor.execute(insert_sql, tuple(row.values))
            

            to_conn.commit()
            close_db(to_conn,cursor)
            close_db(from_conn, None)
            logging.info(f"Data is successfully inserted into target database :{table}")
        return {"status": "success"}


class datacomp(Resource):
    def post(self):
        data = request.get_json()
        PRI_DB = data.get("PRIMARY_DB")
        SE_DB = data.get("SECONDARY_DB")
        tables = data.get("TABLES")
    
        if PRI_DB == "SQL":
            pri_conn =sql_get_connection(PRI_DB)
            logging.info(pri_conn)
        else :
            pri_conn = get_connection(PRI_DB)
            logging.info(pri_conn)
        if SE_DB == "SQL":
            se_conn = sql_get_connection(SE_DB)
            logging.info(se_conn)
        else:
            se_conn = get_connection(SE_DB)
            logging.info(se_conn)
        status = []
        for table in tables:
            query = f"SELECT * FROM {table};"

            pri_df = pd.read_sql(query, pri_conn)
            se_df = pd.read_sql(query, se_conn)

            output = pri_df.equals(se_df)  
            status.append({
                "table": table,
                "match": output
            })
        close_db(pri_conn)
        close_db(se_conn)
        return jsonify(status)
            
        




            