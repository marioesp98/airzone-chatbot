# Function to calculate SHA256 hash for the concatenation of the first three columns
import hashlib
import os
import mysql.connector


def calculate_hash(concatenated_string):
    sha256_hash = hashlib.sha256(concatenated_string.encode()).hexdigest()
    return sha256_hash


def insert_df_into_db(df, query, table_name):
    mysql_user = os.environ.get('MYSQL_USER')
    mysql_password = os.environ.get('MYSQL_PASSWORD')
    mysql_database = os.environ.get('MYSQL_DATABASE')

    # Create a MySQL connection
    connection = mysql.connector.connect(
        host="localhost",
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    # For each row in the dataframe,
    # Create a cursor
    cursor = connection.cursor()

    # Fetch existing hash_ids from the database
    cursor.execute(f"SELECT primary_hash_id FROM {table_name}")
    existing_hash_ids = [row[0] for row in cursor.fetchall()]

    # Filter rows in the DataFrame based on whether the primary hash id and mod hash id already exists
    rows_to_insert = df[~df['primary_hash_id'].isin(existing_hash_ids)]

    # Insert the filtered rows into the MySQL database
    if not rows_to_insert.empty:
        values = [tuple(row) for row in rows_to_insert[
            df.columns.tolist()].values]
        cursor.executemany(query, values)
        connection.commit()

    # Close the connection
    connection.close()