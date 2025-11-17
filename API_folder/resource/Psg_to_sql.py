from flask_restful import Resource, Api, request
from flask import request
# from ..common.config import get_connection

import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
from common.config import  get_connection, sql_get_connection


class psgr_to_sql(Resource):
    def post(self):
        data = request.get_json()
        from_db = data.get("FROM_DB")
        to_db = data.get("TO_DB")
        tables = data.get("TABLES")

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

            insert_sql = f"INSERT INTO `{table}` {col_str} VALUES ({placeholders})"

            cursor = to_conn.cursor()

            for _, row in df.iterrows():
                cursor.execute(insert_sql, tuple(row.values))

            to_conn.commit()

            logging.info(f"Data is successfully inserted into target database :{table}")
        return {"status": "success"}


            