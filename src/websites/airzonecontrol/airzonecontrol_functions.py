import logging
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.general_functions import insert_df_into_db, calculate_hash


def academia_scraper(session, db_connection):
    logging.info("Starting the 'Academia' scraper...")
    try:
        support_endpoint = 'https://api.airzonecloud.com/msacademy.pv1/courses/'
        response = session.get(support_endpoint)
        response_json = response.json()

        courses = response_json['body']['courses']

        # If the categories are found in the response, extract the 'name' values for each list element in
        # 'digital_sections'
        course_list = []
        if courses:
            for course in courses:
                course_title = course['title']
                course_raw_description = course['description']

                # In order to clean the description, we will use BeautifulSoup
                soup = BeautifulSoup(course_raw_description, 'html.parser')
                for p_tag in soup.find_all('p'):
                    if p_tag.find('strong'):
                        p_tag.decompose()
                        break

                # Clean the final description removing double whitespaces and whitespaces before a dot
                course_clean_description = soup.text.lstrip('Webinar').replace(" .", ".").replace("   ", " ").replace(
                    "  ",
                    " ").replace(
                    "\n", " ").strip()

                # Include primary_hash_id and mod_hash_id
                hash_id_data = f"{course_title}{course_clean_description}"
                hash_id = calculate_hash(hash_id_data)

                # Save the data into a dictionary (id, profile, mode, title, description)
                course_dict = {'hash_id': hash_id,
                               'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                               'source': 'Academia',
                               'title': course_title,
                               'description': course_clean_description}
                course_list.append(course_dict)

            logging.info("Inserting the 'Academia' scraped data into a dataframe...")
            df = pd.DataFrame(course_list)

            logging.info("Inserting the 'Academia' data into the database...")
            collection = db_connection['academia']
            insert_df_into_db(collection, df)

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the academia_scraper function: {str(e)}")


def partner_scraper(session, db_connection):
    logging.info("Starting the 'Partner' scraper...")
    try:
        partners_endpoint = 'https://api.airzonecloud.com/mscrm.pv1/crm-associates/categories'
        response = session.get(partners_endpoint)

        partner_list = []
        response_json = response.json()
        partner_categories = response_json['body']['categories']['data']
        for category in partner_categories:
            category_name = category['name']
            for partner in category['associates']:
                name = partner['name']
                alias = partner['alias']
                address = partner['address']
                city = partner['city']
                postal_code = partner['postal_code']
                phone = partner['phone']
                email = partner['email']

                # Include primary_hash_id and mod_hash_id
                hash_id_data = f"{category_name}{name}{address}{city}{postal_code}{phone}{email}{alias}"
                hash_id = calculate_hash(hash_id_data)

                partner_dict = {'hash_id': hash_id,
                                'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'source': 'Partner',
                                'category': category_name,
                                'name': name, 'alias': alias, 'address': address,
                                'city': city, 'postal_code': postal_code, 'phone': phone, 'email': email}

                partner_list.append(partner_dict)

        logging.info("Inserting the 'Partner' scraped data into a dataframe...")

        # Store the data from the partner_list the dataframe
        df = pd.DataFrame(partner_list)

        logging.info("Inserting the 'Partner' data into the database...")

        collection = db_connection['partner']
        insert_df_into_db(collection, df)

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the partner_scraper function: {str(e)}")
