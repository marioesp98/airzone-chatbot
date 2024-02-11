import hashlib
import logging
import os
import re
import time
from typing import List

import requests
import yaml
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pymongo import errors as pymongo_errors


def remove_html_tags(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")
    cleaned_text = soup.get_text()
    return cleaned_text


async def fetch(session, url, output_format='text'):
    """
    Async function to fetch the response for a given URL.
    :param session: aiohttp session
    :param url: URL to fetch
    :param output_format: output format of the response (text or json)
    """
    async with session.get(url) as response:
        if output_format == 'text':
            return await response.text()
        elif output_format == 'json':
            return await response.json()
        else:
            raise ValueError("Invalid output format")


def calculate_hash(concatenated_string):
    """
    Calculate the SHA256 hash for a given string.
    :param concatenated_string: string to calculate the hash
    :return: SHA256 hash
    """
    sha256_hash = hashlib.sha256(concatenated_string.encode('utf-8')).hexdigest()
    return sha256_hash


def insert_df_into_db(collection, df):
    """
    Insert a DataFrame into a given collection in the MongoDB database. Only the documents that do not exist in the
    collection will be inserted, and those that already exist and are not in the dataframe will be removed
    :param collection: database collection object
    :param df: pandas Dataframe
    """
    try:
        source = df['source'][0]
        # Remove duplicate values from the dataframe with the same 'hash_id'
        df.drop_duplicates(subset='hash_id', keep='first', inplace=True)

        # Get the existing hash IDs from the 'Support' collection in the database
        db_hash_ids = collection.distinct('hash_id')

        # Remove rows in the DataFrame that already exist in the database
        rows_to_insert = df[~df['hash_id'].isin(db_hash_ids)]

        # Insert the new rows into the database
        if not rows_to_insert.empty:
            documents_to_insert = rows_to_insert.to_dict(orient='records')
            collection.insert_many(documents_to_insert)

        # Remove from the database the documents that are not in the DataFrame
        rows_to_remove = collection.find({'source': source, 'hash_id': {'$nin': df['hash_id'].to_list()}})
        if rows_to_remove:
            for row in rows_to_remove:
                collection.delete_one({'_id': row['_id']})

        logging.info(
            f"Process finished successfully. {len(rows_to_insert)} new rows were inserted into the '{collection.name}' collection")

    except pymongo_errors.PyMongoError as e:
        logging.info(f"Failed to insert documents into '{collection.name}' collection: {e}")
    except Exception as e:
        raise e


def setup_logging():
    """
    Set up the logging system based on the deployment option (On-premise, Lambda or EC2)
    """
    if os.environ.get('DEPLOYMENT_OPTION') == 'LAMBDA':
        logging.getLogger().setLevel(logging.INFO)
    else:
        with open('src/utils/logger_config.yaml', 'r') as config_file:
            config = yaml.safe_load(config_file)
        # Configure the logging system
        logging.config.dictConfig(config)
        # Get the logger
        logger = logging.getLogger(__name__)


def extract_text_from_pdf(document):
    document_title = document['title']
    document_url = document['url']
    try:
        # Extract the document title from the URL

        # Download the PDF file from the URL
        response = requests.get(document_url)
        response.raise_for_status()  # Check for any errors during download

        # Read the content of the PDF into a BytesIO object
        pdf_bytes = BytesIO(response.content)

        # Create a PdfReader object using the downloaded PDF content
        reader = PdfReader(pdf_bytes)

        print(f"Reading file: {document_title}")
        # read data from the file and put them into a variable called raw_text
        raw_text = ''
        pages_data = []
        with ThreadPoolExecutor() as executor:
            for i, page in enumerate(reader.pages):
                print(f"Reading page {i + 1}...")
                text = executor.submit(page.extract_text)
                if text.result():
                    # Create a tuple with the page number and the text extracted from the page
                    page_data = (i + 1, text.result())
                    # Append the tuple to the list
                    pages_data.append(page_data)

        # Sort the list of tuples by page number
        pages_data.sort(key=lambda tup: tup[0])

        # Extract the text from the sorted list of tuples and concatenate it into a single string
        for page_data in pages_data:
            raw_text += page_data[1] + '\n'

        chunks = split_text_into_chunks(raw_text, chunk_size=1000, chunk_overlap=200)

        chunk_dict_list = []
        for i, text in enumerate(chunks):
            chunk_description = text.replace("\n", " ")
            # Include primary_hash_id and mod_hash_id
            hash_id_data = f"{document_title}{chunk_description}"
            hash_id = calculate_hash(hash_id_data)

            chunk_dict = {
                'hash_id': hash_id,
                'upload_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source': 'Airzone Downloads',
                'title': document_title,
                'url': document_url,
                'index': i + 1,
                'description': chunk_description
            }
            chunk_dict_list.append(chunk_dict)

        return chunk_dict_list

    except Exception as e:
        logging.error(f"Failed to extract content from '{document_url}': {e}")
        return []


def find_missing_documents_in_db(db, collection_name, field_name, document_list):
    """
    Find the documents that are not in the database given a field to search by and a list of documents, and return them
    in a list
    :param db: database object
    :param collection_name: collection name
    :param field_name: field name to search by
    :param document_list: list of documents to search
    :return: list of documents that are not in the database
    """
    try:
        # Connect to MongoDB
        collection = db[collection_name]

        # Query MongoDB and remove from the list the items that already exist in the database
        existing_urls = collection.find({field_name: {'$in': [d[field_name] for d in document_list]}})

        existing_urls_list = [doc[field_name] for doc in existing_urls]

        missing_items = [d for d in document_list if d[field_name] not in existing_urls_list]

        return missing_items

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Recursive function to extract text from nested dictionaries
def extract_json_text(obj):
    text = ""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "title" and isinstance(value, str):
                text = value + ": " + text
            elif isinstance(value, str):
                text += value + ". "  # Add the prefix before the text
            elif isinstance(value, dict):
                text += extract_json_text(value)
    elif isinstance(obj, list):
        for item in obj:
            text += extract_json_text(item)

    # Clean the final text removing double whitespaces and whitespaces before a dot
    clean_text = text.replace("..", ".").replace(" .", ".").replace("   ", " ").replace("  ", " ").replace("\n",
                                                                                                           " ").strip()

    return clean_text


# Create a function to split a text into chunks using CharacterTextSplitter
def split_text_into_chunks(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    """
    Split a text into chunks using CharacterTextSplitter
    :param text: text to split
    :param chunk_overlap: chunk overlap size
    :param chunk_size: chunk size
    :return: list of chunks
    """
    clean_text = remove_html_tags(text)
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
    )
    chunks = text_splitter.split_text(clean_text)

    return chunks
