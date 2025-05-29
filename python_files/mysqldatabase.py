import logging
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename='logs/database.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

un = "root"
pw = os.environ["SQL_ROOT_PASSWORD"]
ht = "localhost"
db = "project_olap"

class Database:
    """
    This class will help us in communicating with our database.
    """

    def __init__(self, username = un, password = pw, host = ht, database = db):
        try:
            temp_engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}")
            with temp_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database}"))
                logging.info(f"Database '{database}' ensured to exist.")
            temp_engine.dispose()
        except Exception as e:
            logging.error(f"Couldn't create temp_engine: {e}")

        try:
            self.engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
            logging.info("Successfully created an engine")
        except Exception as e:
            logging.error(f"Couldn't create the engine: {e}")

    def import_table(self, table_name):
        """
        This method will import a table into the database.
        :param table_name: the table to be imported
        :return: the imported table
        """

        try:
            df = pd.read_sql_table(table_name, con = self.engine)
            df = df.replace({None: np.nan})                 # map None values, which are the default return value for NULL values in database, to np.nan
            logging.info(f"Successfully imported the table: {table_name}")
            return df
        except Exception as e:
            logging.error(f"Couldn't import the table: {table_name} due to {e}")

    def create_table(self, df, table_name):
        """
        This method will create a new table in the database
        :param df: dataframe to be uploaded
        :param table_name: the name of the table
        """

        try:
            df.to_sql(table_name, con = self.engine, if_exists='replace', index=False)
            logging.info(f"Successfully created the table: {table_name}")
        except Exception as e:
            logging.error(f"Couldn't import the table: {table_name} due to {e}")

    def execute_query(self, query):
        """
        This method will execute a query and return the results.
        :param query: a sql query
        :return: the resulting table
        """
        try:
            df = pd.read_sql_query(query, con = self.engine)
            df = df.replace({None: np.nan})
            logging.info(f"Successfully executed the query: {query}")
            return df
        except Exception as e:
            logging.error(f"Couldn't execute the query: {query} due to {e}")
            return e

    def close(self):
        """
        This method will close the database connection
        """

        try:
            self.engine.dispose()
            logging.info("Engine disposed and all connections closed.")
        except Exception as e:
            logging.error(f"Error disposing engine: {e}")
