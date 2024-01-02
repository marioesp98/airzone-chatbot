import hashlib
import logging
import os

import mysql.connector
import yaml


def calculate_hash(concatenated_string):
    """
    Calculate the SHA256 hash for a given string.
    :param concatenated_string:  string to calculate the hash
    :return: SHA256 hash
    """
    sha256_hash = hashlib.sha256(concatenated_string.encode('utf-8')).hexdigest()
    return sha256_hash


def insert_df_into_db(db_connection, df, query, table_name):
    """
    Insert a DataFrame into a given table in the database.
    :param db_connection: MySQL's connection object
    :param df: pandas Dataframe
    :param query: SQL query for insertion
    :param table_name: name of the table in the database
    """
    global cursor
    try:
        # For each row in the dataframe,
        # Create a cursor
        cursor = db_connection.cursor()

        # Remove duplicate values from the dataframe by column 'primary_hash_id'
        df.drop_duplicates(subset='primary_hash_id', keep='first', inplace=True)

        # Fetch existing hash_ids from the database
        cursor.execute(f"SELECT primary_hash_id FROM {table_name}")
        existing_hash_ids = [row[0] for row in cursor.fetchall()]

        # Filter rows in the DataFrame based on whether the primary hash id and mod hash id already exists
        rows_to_insert = df[~df['primary_hash_id'].isin(existing_hash_ids)]

        # Insert the filtered rows into the database
        if not rows_to_insert.empty:
            values = [tuple(row) for row in rows_to_insert[
                df.columns.tolist()].values]
            cursor.executemany(query, values)
            db_connection.commit()

        logging.info(f"Process finished successfully. {len(rows_to_insert)} new rows were inserted into the '{table_name}' table")

    except mysql.connector.Error as e:
        logging.info(f"Failed to insert dataframe rows into '{table_name}' table: {e}")
    except Exception as e:
        raise e

    finally:
        cursor.close()


# Create a function that sets up the logging based on the logger_config.yaml file if
# the environment variable DEPLOYMENT is set to 'PREMISE' or a normal logging if it is set to 'CLOUD'
def setup_logging():
    if os.environ.get('DEPLOYMENT_OPTION') == 'CLOUD':
        logging.getLogger().setLevel(logging.INFO)
    else:
        with open('src/utils/logger_config.yaml', 'r') as config_file:
            config = yaml.safe_load(config_file)
        # Configure the logging system
        logging.config.dictConfig(config)
        # Get the logger
        logger = logging.getLogger(__name__)
