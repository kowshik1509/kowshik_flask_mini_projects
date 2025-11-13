from flask_restful import Resource, Api, request
from flask import request
# from ..common.config import get_connection

import pandas as pd
import numpy as np
from flask import jsonify, json
import logging
import datetime
from datetime import datetime
from resource.utils import table_exists, METHODS
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
from common.config import connectionpool, get_connection
from common import config

class datafetch(Resource):
    def get(self):
        conn = False
        try:
            conn = connectionpool.dev_connection()
            logging.info("Connection Established")
            Query = "select * from order_lines"
            df = pd.read_sql(Query, conn)
            config.connectionpool.postgres_dev_pool['pool'].putconn(conn)
            logging.info("Connection Close ")
            return df.to_html()
        except Exception as e:
            return f"Error : {e}"


class tabledata(Resource):
    def post(self):
        conn = connectionpool.dev_connection()
        data = request.get_json()
        table = data.get("Table")
        Query = f"select * from {table}"
        df = pd.read_sql(Query, conn)
        config.connectionpool.postgres_dev_pool['pool'].putconn(conn)
        logging.info("Connection Closed ")
        return df.to_html()
    
class datacreate(Resource):
    def post(self):
        retrive = request.get_json()
        db, table, values = retrive.get("Database_name"), retrive.get("Table_name"), retrive.get("Values")
        if not all([db, table, values]):
            return jsonify({"error": "Missing key values"}), 400
        try:
            cursor = None
            db_name = db.upper()
            if db_name == "DEV":
                conn = connectionpool.dev_connection()
            elif db_name == "PROD":
                conn = connectionpool.prod_connection()
            elif db_name == "BACKEND":
                conn = connectionpool.backend_connection()
            else:
                return None
            if not conn:
                return {"error": "Failed to connect to database"}, 500
            cursor = conn.cursor()
            logging.info(conn)
            if not table.isidentifier():
                return {"error": "Invalid table name"}, 400
            cols = ', '.join(values.keys())
            placeholders = ', '.join(['%s'] * len(values))
            cursor.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", tuple(values.values()))
            conn.commit()
            logging.info(f"Data is inserted into {table}")
            return f"Inserted data into {table} successfully"
        except Exception as e:
            return f"error : {e}"
        finally:
            if cursor:
                cursor.close()
            if conn:
                if db_name == "DEV":
                    config.connectionpool.postgres_dev_pool['pool'].putconn(conn)
                elif db_name == "PROD":
                    config.connectionpool.postgres_prod_pool['pool'].putconn(conn)
                elif db_name == "BACKEND":
                    config.connectionpool.postgres_backend_pool['pool'].putconn(conn)
                else:
                    return None
                
            logging.info(f"{conn} is closed")
    
class dataread(Resource):
    def post(self):
        retrive = request.get_json()
        db, table, conditions = retrive.get("Database_name"), retrive.get("Table_name"), retrive.get("Conditions")
        if not all([db, table]):
            logging.info("Values missing, Please Check !!!")
            return {"error": "Missing key values"}, 400
        conn = cursor = None
        try:
            conn = get_connection(db)
            cursor = conn.cursor()
            print(conn)
            if conditions:
                where = ' AND '.join([f"{k}=%s" for k in conditions.keys()])
                cursor.execute(f"SELECT * FROM {table} WHERE {where}", tuple(conditions.values()))
            else:
                cursor.execute(f"SELECT * FROM {table}")

            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            logging.info(f"Data is read from {table}")
            return f"data: {[dict(zip(cols, r)) for r in rows]}", 200
        except Exception as e:
            return f"error: {e}", 500
        finally:
            if conn:
                conn.close()

            if cursor:
                cursor.close()

            logging.info(f"{conn} is closed")


class session(Resource):
    def post(self):
        data = request.get_json()
        conn = get_connection("BACKEND")
        cursor = conn.cursor()
        query = "select user_name, user_password from user_authentication;"
        user_name = data.get("USER_NAME")
        password = data.get("PASSWORD")
        session = data.get("SESSION")
        df = pd.read_sql(query, conn)
        logging.info(df)
        user_found = False
        password_correct = False
        for index, row in df.iterrows():
            if row["user_name"] == user_name:
                logging.info("USER EXISTS")
                user_found = True
                if row["user_password"] == password:
                    logging.info("USER LOGINED")
                    query_2 = f"select user_id from user_authentication where user_name = %s "
                    df2 = pd.read_sql(query_2,conn, params=[user_name])
                    user_id = df2.iloc[0]["user_id"]
                    user_id = int(user_id)
                    logging.info(user_id)
                    password_correct = True
                    if session.upper() == "LOGIN":
                        login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        query_2 = "insert into login_transaction_table (user_name, user_id, login_time) values (%s, %s, %s);"
                        cursor.execute(query_2,(user_name,user_id, login_time))
                        conn.commit()
                        return f"{user_name} login sucessfull"
                    elif session.upper() == "LOGOUT":
                        logout_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        status = "Inactive"
                        query_update = """
                                UPDATE login_transaction_table
                                SET logout_time = %s, status = %s
                                WHERE user_id = %s AND status = 'Active';
                            """
                        cursor.execute(query_update, (logout_time, status, user_id))
                        conn.commit()
                        return f"{user_name} logout sucessfull"

                else:
                    logging.info("USER NOT EXISTS")
                
        if user_found and not password_correct:
                return jsonify({"message": f"User {user_name} exists but login failed (wrong password)"}), 401
        elif not user_found:
                return jsonify({"message": "User or password not found"}), 404
        
class login(Resource):
    def post(self):
        data = request.get_json()
        conn = get_connection("BACKEND")
        cursor = conn.cursor()
        query = "select user_name, user_password from user_authentication;"
        user_name = data.get("USER_NAME")
        password = data.get("PASSWORD")
        df = pd.read_sql(query, conn)
        logging.info(df)
        user_found = False
        password_correct = False
        for index, row in df.iterrows():
            if row["user_name"] == user_name:
                logging.info("USER EXISTS")
                user_found = True
                if row["user_password"] == password:
                    logging.info("USER LOGINED")
                    query_2 = f"select user_id from user_authentication where user_name = %s "
                    df2 = pd.read_sql(query_2,conn, params=[user_name])
                    user_id = df2.iloc[0]["user_id"]
                    user_id = int(user_id)
                    logging.info(user_id)
                    login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    query_2 = "insert into login_transaction_table (user_name, user_id, login_time) values (%s, %s, %s);"
                    cursor.execute(query_2,(user_name,user_id, login_time))
                    conn.commit()
                    return f"{user_name} login sucessfull"
                else:
                    logging.info("USER NOT EXISTS")
                
        if user_found and not password_correct:
                return jsonify({"message": f"User {user_name} exists but login failed (wrong password)"}), 401
        elif not user_found:
                return jsonify({"message": "User or password not found"}), 404
        
class logout(Resource):
    def post(self):
        data = request.get_json()
        conn = get_connection("BACKEND")
        cursor = conn.cursor()
        query = "select user_name, user_password from user_authentication;"
        user_name = data.get("USER_NAME")
        password = data.get("PASSWORD")
        df = pd.read_sql(query, conn)
        logging.info(df)
        user_found = False
        password_correct = False
        for index, row in df.iterrows():
            if row["user_name"] == user_name:
                logging.info("USER EXISTS")
                user_found = True
                if row["user_password"] == password:
                    logging.info("USER LOGINED")
                    query_2 = f"select user_id from user_authentication where user_name = %s "
                    df2 = pd.read_sql(query_2,conn, params=[user_name])
                    user_id = df2.iloc[0]["user_id"]
                    user_id = int(user_id)
                    logging.info(user_id)
                    password_correct = True
                    logout_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    status = "Inactive"
                    query_update = """
                                UPDATE login_transaction_table
                                SET logout_time = %s, status = %s
                                WHERE user_id = %s AND status = 'Active';
                            """
                    cursor.execute(query_update, (logout_time, status, user_id))
                    conn.commit()
                    return f"{user_name} logout sucessfull"

                else:
                    logging.info("USER NOT EXISTS")
                
        if user_found and not password_correct:
                return jsonify({"message": f"User {user_name} exists but login failed (wrong password)"}), 401
        elif not user_found:
                return jsonify({"message": "User or password not found"}), 404

class json_to_df(Resource):
    def post(self):
            # Data retriving from json payload
            data = request.get_json()
            division_id = data.get("DIVISION")
            mode = data.get("MODE")
            header_details = data.get("HEADER_DETAILS",[])
            print(division_id)
            print(mode)
            print(header_details)
            # Normalize. pd.serialize
            # Looping through each line in  header_details
            # for header in header_details: # 
            #     po_number = header.get("PO_NUMBER")
            #     print(po_number)

            #     line_details = header.get("LINE_DETAILS",[])
            #     print(line_details)

            #     # Looping through each line in line_details
            #     for detail in line_details:
            #         po_line_id = detail.get("PO_LINE_ID")
            #         print(po_line_id)

            #         stop_id = detail.get("STOP")
            #         print(stop_id)
            #         # Adding data into empty list/ original data list
            #         original_data.append({
            #             "DIVSION_ID": division_id,
            #             "MODE": mode,
            #             "PO_NUMBER":po_number,
            #             "PO_LINE_ID": po_line_id,
            #             "STOP_ID": stop_id
            #         })
            # 
            df = pd.json_normalize(
                header_details,
                record_path=["LINE_DETAILS"],
                meta=["PO_NUMBER"],       
                errors='ignore'
            )
            df["DIVISION_ID"] = division_id
            df["MODE"] = mode 
            df.rename(columns={
                "PO_LINE_ID": "PO_LINE_ID",
                "STOP": "STOP_ID"
            }, inplace=True)
            # Conversion of data into DF
            # df = pd.DataFrame(original_data)   
            # print(df)
            # Df conversion into json serializable dictionary 
            data_json = df.to_dict(orient="records")

            # returning response in json format
            return jsonify(data_json)

    
class fromdb_todb(Resource):
    def post(self):
        try:
            payload = request.get_json()
            from_db_name = payload.get("from_db")
            to_db_name = payload.get("to_db")
            Tables = payload.get("Tables")

            if not from_db_name or not to_db_name:
                return jsonify({"error": "from_db and to_db must be provided"}), 400

            # Connection Establishment
            from_conn = get_connection(from_db_name.upper())
            logging.info(f"\nFrom_DB connection established : {from_conn}\n")
            to_conn = get_connection(to_db_name.upper())
            logging.info(f"To_conn_db Connection Established : {to_conn}\n")
            from_cur = from_conn.cursor()
            to_cur = to_conn.cursor()
            summary = {}
            # if Tables are not provided in the payload it checks for the tables in the from_db
            if not Tables:
                # prevoius logic static table names
                # Tables = [ "order_headers", "order_lines", "invoice_headers", "invoice_lines", "customer_headers", "customer_lines" ]
                logging.info("\n Checking existing tables in From  DB\n")

                with from_conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public';
                    """)
                    Tables = [row[0] for row in cur.fetchall()]
                    logging.info(f"Tables Existed in {from_db_name} : {Tables}")

            # For every table in the Table it retrives data of column names and their data types
            for table in Tables:
                if not table_exists(to_conn, table, schema='public'):
                    logging.info(f"\nCreating structure for table: {table}\n")
                    from_cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position;
                    """, (table,))
                    columns = from_cur.fetchall()
                    # if no columns present in the table
                    if not columns:
                        logging.warning(f"No columns found for {table}, skipping.")
                        continue
                    # Creation of table
                    ddl_parts = [f"CREATE TABLE {table} ("]
                    ddl_parts += [f"    {col} {dtype}," for col, dtype in columns]
                    ddl_parts[-1] = ddl_parts[-1].rstrip(',')
                    ddl_parts.append(");")

                    ddl = "\n".join(ddl_parts)
                    to_cur.execute(ddl)
                    logging.info(f"{table} created successfully.")
                else:
                    logging.info(f"{table} already exists in target database.")

            to_conn.commit()

    # Copies Data from tables 
            # previous logic reads queries from yaml file
            # queries = yaml_queries()
            # for table, query in queries.items():
            #     logging.info(f"Copying data for {table} ...")
            #     df = pd.read_sql(query, from_conn)
            for table in Tables:
                logging.info(f"Copying data for {table} ...")
                # Reads Data of every table in Tables
                query = f"select * from {table}"
                df = pd.read_sql(query, from_conn)

                if df.empty:
                    summary[table] = 0
                    continue
                # Select id_cols in df
                id_cols = [col for col in df.columns if 'order_id' in col.lower()]
                # Converting the order_id into sting type and adding 101
                for col in id_cols:
                    df[col] = df[col].astype(str).apply(lambda x: f"101{x}")

                # Insert into PRod_DB
                cols = ",".join(df.columns)

                # values "," and joining them 
                placeholders = ",".join(["%s"] * len(df.columns))
                
                insert_query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
                # Df iterations to insert Data
                for _, row in df.iterrows():
                    to_cur.execute(insert_query, tuple(row))

                # Commit
                to_conn.commit()
                summary[table] = len(df)
                logging.info(f"\n{len(df)} rows inserted into {table}.\n")

            # Close connections
            from_cur.close()
            to_cur.close()
            from_conn.close()
            to_conn.close()
            logging.info("Connections closed")
            return {
                "message": "DataBase Sync is Completed Sucessfully !!!",
                "details": summary
            }, 200
    # To_dict and json difference 

        except Exception as j:
            print("Error:", j)
            
            return {"error": str(j)}, 500

class call_method(Resource):
    def post(self):
        data = request.get_json()
        if not data or "method" not in data:
            return {"error": "Missing 'method' in payload"}, 400
        # Post method
        method_name = data["method"].upper() # Insert, delete, update, create
        params = data.get("params", {})
        # Input path ( call method )
        func = METHODS.get(method_name)
        if not func:
            return {"error": f"Method '{method_name}' not found"}, 404

        try:
            result = func(**params)

            if isinstance(result, pd.DataFrame):
                result = result.astype(object).where(pd.notnull(result), None).to_dict(orient="records")
            elif isinstance(result, np.ndarray):
                result = result.tolist()
            elif isinstance(result, (np.generic,)):
                result = result.item()

            return result, (500 if isinstance(result, dict) and "error" in result else 200)

        except TypeError as e:
            return {"error": f"Invalid parameters: {str(e)}"}, 400
        except Exception as e:
            return {"error": str(e)}, 500
