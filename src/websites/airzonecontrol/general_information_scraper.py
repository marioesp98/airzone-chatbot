import logging
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.general_functions import calculate_hash, insert_df_into_db, extract_json_text, split_text_into_chunks


def general_information_scraper(session, db):
    logging.info("Starting the General Information scraper...")
    try:
        translations_endpoint = 'https://api.airzonecloud.com/msairpress.pv1/translations/es-ES'
        response = session.get(translations_endpoint)
        response_json = response.json()
        general_information_list = []

        # Iterate over the items in the "body" object
        for key, value in response_json["body"].items():
            text = extract_json_text(value)

            chunks = split_text_into_chunks(text, chunk_size=500, chunk_overlap=100)

            for i, text in enumerate(chunks):
                hash_id_data = f"{key}{text}"
                hash_id = calculate_hash(hash_id_data)

                chunk_dict = {
                    'hash_id': hash_id,
                    'upload_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'source': 'Airzone Control',
                    'title': key,
                    'description': text
                }
                general_information_list.append(chunk_dict)

        # Next, get the footer sections from Myzone website
        endpoints = (('Política de privacidad', 'https://myzone.airzone.es/politica-privacidad'),
                     ('Condiciones de uso', 'https://myzone.airzone.es/condiciones-uso'),
                     ('Aviso legal', 'https://myzone.airzone.es/aviso-legal'),
                     ('Política de cookies', 'https://myzone.airzone.es/politica-cookies'))

        footer_list = []
        for endpoint in endpoints:
            response = session.get(endpoint[1])
            soup = BeautifulSoup(response.text, 'html.parser')
            sections = soup.select('div.col-md-10 > div.row')
            for section in sections:
                description = ''
                section_name = None
                for tag in section.contents:
                    if tag.name == 'h5':
                        section_name = tag.text.strip().capitalize()
                    elif tag.name == 'p':
                        description += tag.text.strip() + " "
                    elif tag.name == 'ul' or tag.name == 'ol':
                        list_number = 1
                        for li in tag.contents:
                            if li.name == 'li':
                                description += str(list_number) + ") " + li.text.strip() + " "
                                list_number += 1

                if section_name is not None:
                    clean_description = re.sub(r'\s+', ' ', description).strip()
                    footer_list.append((endpoint[0], clean_description))

        for item in footer_list:
            chunks = split_text_into_chunks(item[1], chunk_size=1000, chunk_overlap=200)

            for i, text in enumerate(chunks):
                hash_id_data = f"{key}{text}"
                hash_id = calculate_hash(hash_id_data)

                chunk_dict = {
                    'hash_id': hash_id,
                    'upload_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'source': 'Myzone',
                    'title': item[0],
                    'description': text
                }
                general_information_list.append(chunk_dict)

        logging.info("Inserting the General Information scraped data into a dataframe...")
        # Store the categories, units, subunits and descriptions in the dataframe
        df = pd.DataFrame(general_information_list)

        logging.info("Inserting the General Information data into the database...")
        collection = db['general_information']
        insert_df_into_db(collection, df)

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the academia_scraper function: {str(e)}")
