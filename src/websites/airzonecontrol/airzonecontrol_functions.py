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
                course_category = (course['category']['course_category_iso']).lower()
                course_profile = (course['profile']['course_profile_iso']).lower()
                course_mode = (course['mode']['course_mode_iso']).lower()
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
                primary_hash_id_data = f"{course_category}{course_profile}{course_mode}{course_title}"
                primary_hash_id = calculate_hash(primary_hash_id_data)

                mod_hash_id_data = course_clean_description
                mod_hash_id = calculate_hash(mod_hash_id_data)

                # Save the data into a dictionary (id, profile, mode, title, description)
                course_dict = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                               'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                               'category': course_category, 'profile': course_profile, 'mode': course_mode,
                               'title': course_title,
                               'description': course_clean_description}
                course_list.append(course_dict)

            logging.info("Inserting the 'Academia' scraped data into a dataframe...")
            df = pd.DataFrame(course_list)

            query = "INSERT INTO academia (primary_hash_id, mod_hash_id, uploaded_date, category, profile, mode, " \
                    "title, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

            logging.info("Inserting the 'Academia' data into the database...")
            insert_df_into_db(db_connection, df, query, 'academia')

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
                company_type = partner['crm_company_type']['name']
                name = partner['name']
                alias = partner['alias']
                address = partner['address']
                city = partner['city']
                postal_code = partner['postal_code']
                phone = partner['phone']
                email = partner['email']

                # Include primary_hash_id and mod_hash_id
                primary_hash_id_data = f"{category_name}{name}{company_type}{address}{city}{postal_code}"
                primary_hash_id = calculate_hash(primary_hash_id_data)

                mod_hash_id_data = f"{phone}{email}{alias}"
                mod_hash_id = calculate_hash(mod_hash_id_data)
                partner_dict = {'primary_hash_id': primary_hash_id,
                                'mod_hash_id': mod_hash_id,
                                'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'category': category_name,
                                'company_type': company_type,
                                'name': name, 'alias': alias, 'address': address,
                                'city': city, 'postal_code': postal_code, 'phone': phone, 'email': email}

                partner_list.append(partner_dict)

        logging.info("Inserting the 'Partner' scraped data into a dataframe...")

        # Store the data from the partner_list the dataframe
        df = pd.DataFrame(partner_list)

        query = "INSERT INTO partner (primary_hash_id, mod_hash_id, uploaded_date, category, company_type, name, " \
                "alias, address, city, postal_code, phone, email) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                "%s, %s)"

        logging.info("Inserting the 'Partner' data into the database...")
        insert_df_into_db(db_connection, df, query, 'partner')

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the partner_scraper function: {str(e)}")
