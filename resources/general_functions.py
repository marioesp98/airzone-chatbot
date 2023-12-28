import hashlib
import os
import mysql.connector


# Function to calculate SHA256 hash for the concatenation of the columns of interest
def calculate_hash(concatenated_string):
    sha256_hash = hashlib.sha256(concatenated_string.encode('utf-8')).hexdigest()
    return sha256_hash


# Function to insert a dataframe into a given table in the database. It will only insert the rows that are not
# already in the database
def insert_df_into_db(df, query, table_name):
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_database = os.environ.get('DB_DATABASE')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')

    # Create a MySQL connection
    connection = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_database
    )
    # For each row in the dataframe,
    # Create a cursor
    cursor = connection.cursor()

    # Remove duplicate values from the dataframe by column 'primary_hash_id'
    df.drop_duplicates(subset='primary_hash_id', keep='first', inplace=True)

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
