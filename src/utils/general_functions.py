import concurrent
import hashlib
import logging
import os
import tempfile
from typing import List
import fitz
import requests
import yaml
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pymongo import errors as pymongo_errors


def remove_html_tags(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")
    cleaned_text = soup.get_text()
    return cleaned_text


async def fetch(session, url, output_format='text') -> str:
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


def calculate_hash(concatenated_string) -> str:
    """
    Calculate the SHA256 hash for a given string.
    :param concatenated_string: string to calculate the hash
    :return: SHA256 hash
    """
    sha256_hash = hashlib.sha256(concatenated_string.encode('utf-8')).hexdigest()
    return sha256_hash


def insert_df_into_db(collection, df) -> None:
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
            f"Process finished successfully. {len(list(rows_to_remove))} rows were removed and {len(rows_to_insert)} new "
            f"rows were inserted into the '{collection.name}' collection")

    except pymongo_errors.PyMongoError as e:
        logging.info(f"Failed to insert documents into '{collection.name}' collection: {e}")
    except Exception as e:
        raise e


def setup_logging() -> None:
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


def extract_text_from_pdf(pdf) -> tuple:
    """
    Extract text from a PDF given its URL.
    :param pdf: tuple with the title and the URL of the PDF
    :return: tuple with the title and the extracted text
    """
    title = pdf[1]
    url = pdf[2]

    logging.info(f"Extracting text from PDF: {url}")
    # Download the PDF content
    response = requests.get(url)

    # Creation of a temporary file to store the pdf
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(response.content)
        temp_file_path = temp_file.name

    with fitz.open(temp_file_path) as pdf_document:
        # Initialize an empty string to store the extracted text
        text = ""

        # Iterate through all the pages and extract text
        for page_number in range(len(pdf_document)):
            page = pdf_document.load_page(page_number)
            text += page.get_text()

    os.remove(temp_file_path)

    return title, url, text


# Function to extract text from multiple PDFs in parallel
def extract_text_from_pdfs_parallel(pdf_df) -> List[str]:
    """
    Extract text from multiple PDFs in parallel.
    :param pdf_df: dataframe with the title and the URL of the PDFs
    :return: list with the extracted text of each PDF
    """
    extracted_texts = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each PDF URL
        futures = [executor.submit(extract_text_from_pdf, pdf) for pdf in pdf_df.itertuples()]
        # Get results as they become available
        for future in concurrent.futures.as_completed(futures):
            try:
                extracted_texts.append(future.result())
            except Exception as e:
                print(f"An error occurred: {e}")

    return extracted_texts


def find_missing_documents_in_db(db, collection_name, field_name, document_list) -> List[dict]:
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
        existing_documents = collection.find({field_name: {'$in': [d[field_name] for d in document_list]}})

        existing_document_fields = [doc[field_name] for doc in existing_documents]

        missing_documents = [d for d in document_list if d[field_name] not in existing_document_fields]

        return missing_documents

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Recursive function to extract text from nested dictionaries
def extract_json_text(obj) -> str:
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
