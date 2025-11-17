import psycopg2
from psycopg2 import pool
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
import pymysql

time = datetime.now()

formatted_time = time.strftime("%y-%m-%d")
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt= '%y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"API_folder/common/app_{formatted_time}.log", mode='a'),  # Log to file (append mode)
        logging.StreamHandler()  # Log to console
    ]
)

logger = logging.getLogger("MyApp")


class connectionpool():
    postgres_dev_pool = {}
    postgres_prod_pool = {}
    postgres_backend_pool = {}
    def dev_connection(connections):
        try:
            user = 'postgres'
            password = 'Admin'
            database = 'Dev_db'
            # post = 5432
            # host = 'localhost'
            connections = None
            dev_db_details = f"postgresql://{user}:{password}@localhost:5432/{database}"

            postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool( 1, 3, dev_db_details)
            if connectionpool.postgres_dev_pool == {}:
                connectionpool.postgres_dev_pool['pool'] = postgreSQL_pool
            conn = connectionpool.postgres_dev_pool['pool'].getconn()

            return conn
        
        except Exception as e:
            print(f"Error at connection : {e}")
            return False
    
    def prod_connection(connections):
        try:
            user = 'postgres'
            password = 'Admin'
            database = 'prod_db'
            # post = 5432
            # host = 'localhost'
            connections = None
            dev_db_details = f"postgresql://{user}:{password}@localhost:5432/{database}"

            postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool( 1, 3, dev_db_details)
            if connectionpool.postgres_prod_pool == {}:
                connectionpool.postgres_prod_pool['pool'] = postgreSQL_pool
            conn = connectionpool.postgres_prod_pool['pool'].getconn()

            return conn
        
        except Exception as e:
            print(f"Error at connection : {e}")
            return False

    def backend_connection(connections):
        try:
            user = 'postgres'
            password = 'Admin'
            database = 'backend'
            # post = 5432
            # host = 'localhost'
            connections = 0
            dev_db_details = f"postgresql://{user}:{password}@localhost:5432/{database}"

            postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool( 1, 3, dev_db_details)
            if connectionpool.postgres_backend_pool == {}:
                connectionpool.postgres_backend_pool['pool'] = postgreSQL_pool
            conn = connectionpool.postgres_backend_pool['pool'].getconn()

            return conn
        
        except Exception as e:
            print(f"Error at connection : {e}")
            return False


def get_connection(db_name):
        try:
            db_name = db_name.upper()
            conn = psycopg2.connect(
                user=os.getenv(f"{db_name}_DB_USER"),
                password=os.getenv(f"{db_name}_DB_PASSWORD"),
                host=os.getenv(f"{db_name}_DB_HOST"),
                port=os.getenv(f"{db_name}_DB_PORT"),
                database=os.getenv(f"{db_name}_DB_NAME")
            )
            logger.debug(f"Connected to {db_name} database.")
            return conn
        except Exception as e:
            logger.debug(f"Error while connecting to {db_name} database: {e}")
            return None
        
def sql_get_connection(db_name):
        try:
            db_name = db_name.upper()
            conn = pymysql.connect(
                user=os.getenv(f"{db_name}_DB_USER"),
                password=os.getenv(f"{db_name}_DB_PASSWORD"),
                host=os.getenv(f"{db_name}_DB_HOST"),
                port=int(os.getenv(f"{db_name}_DB_PORT")),
                database=os.getenv(f"{db_name}_DB_NAME")
            )
            logger.debug(f"Connected to {db_name} database.")
            return conn
        except Exception as e:
            logger.debug(f"Error while connecting to {db_name} database: {e}")
            return None
        

def close_db(conn, cursor = None):
    try:
        if cursor:
            cursor.close()
    except Exception as e:
        print(f"Error closing cursor: {e}")

    try:
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error closing connection: {e}")
