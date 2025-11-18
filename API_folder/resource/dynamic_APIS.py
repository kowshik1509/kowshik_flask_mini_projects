from flask_restful import Resource, Api, request
from flask import request
# from ..common.config import get_connection

import pandas as pd
import numpy as np
from flask import jsonify
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
from common.config import  get_connection, sql_get_connection, close_db


class data_insertion(Resource):
    def post(self):
        data = request.get_json()
        from_db = data.get("FROM_DB_NAME")
        logging.info(from_db)
        to_db = data.get("TO_DB_NAME")
        logging.info(to_db)
        table = data.get("TABLE_NAME")
        logging.info(table)
        columns = data.get("COLUMNS", [])
        # values = data.get("VALUES", [])
        if not from_db or not to_db or not table :
            logging.error("All parameters should be provided")
            msg = "FROM_DB_NAME, TO_DB_NAME & TABLE_NAME should be provided"
            return jsonify(msg)
        # if len(columns) != len(values):
        #     logging.error("Length of provided columns and values are not equal")
        #     msg = "Length of provided columns and values are not equal"
        #     return jsonify(msg)
        from_db_conn = get_connection(from_db)
        to_db_conn = get_connection(to_db)
        logging.info(from_db_conn)
        logging.info(to_db_conn)
        if from_db_conn and to_db_conn :
            logging.info("connection established")
        else:
            logging.info("connection failed to establish !!!")
        if columns:
            col_sql = ", ".join(columns)
            data_read_query = f"SELECT {col_sql} FROM {table};"
        else:
            data_read_query = f"SELECT * FROM {table};"
        df = pd.read_sql(data_read_query, from_db_conn)
        logging.info(df)
        if df.empty:
            msg = "No data is found in the database"
            return jsonify(msg)
        placeholders = "(" + ",".join(["%s"] * len(df.columns)) + ")"
        insert_cols = "(" + ", ".join(df.columns) + ")"

        insert_query = f"INSERT INTO {table} {insert_cols} VALUES {placeholders}"

        tgt_cursor = to_db_conn.cursor()
        tgt_cursor.executemany(insert_query, df.to_records(index=False).tolist())
        to_db_conn.commit()
        msg = f"Values are successfully inserted into target db_table {to_db}, {table}"
        return jsonify(msg)
