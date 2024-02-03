import logging
import time
import pandas as pd
import requests
import warnings
from bs4 import BeautifulSoup, NavigableString
from requests import Session
from src.utils.general_functions import calculate_hash, insert_df_into_db, extract_text_from_pdf, \
    find_missing_documents_in_db
from bs4 import MarkupResemblesLocatorWarning

# Disable the warning
warnings.simplefilter("ignore", MarkupResemblesLocatorWarning)


def airzone_support_scraper(session: Session) -> pd.DataFrame:
    support_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/digital-doc/books'
    support_document_list = []
    categories = []
    target_iso = 'MU_AZCLOUD'

    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['hash_id', 'uploaded_date', 'source', 'title', 'description'])

    try:
        logging.info("Starting the 'Airzone Support' website scraper...")
        response = session.get(support_endpoint)
        response_json = response.json()

        digital_books_data = response_json['body']['digital_books']['data']

        category_list = next((item for item in digital_books_data if item.get('az_iso') == target_iso), None)

        # If the categories are found in the response, the 'name' values for each list element in
        # 'digital_sections' are extracted
        if category_list and 'digital_sections' in category_list:
            for category in category_list['digital_sections']:
                # Add the category name and az_iso into a dictionary
                categories.append({'name': category['name'], 'az_iso': category['az_iso']})

            for category in categories:
                category_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/digital-doc/sections/{category['az_iso']}"
                response = session.get(category_endpoint)
                response.raise_for_status()
                response_json = response.json()
                units = response_json['body']['digital_section']['digital_subsections']

                for unit in units:
                    for subunit in unit['digital_contents']:
                        title = subunit['name']
                        raw_description = subunit['description']

                        # In order to clean the description, we will use BeautifulSoup
                        soup = BeautifulSoup(raw_description, 'html.parser')
                        clean_description = ""
                        excluded_tags = ['a']
                        for tag in soup.contents:
                            if tag.name in excluded_tags:
                                tag.decompose()
                            elif tag.next.name in excluded_tags:
                                tag.next.decompose()
                            elif tag.text == '':
                                tag.decompose()
                            elif isinstance(tag, NavigableString):
                                clean_description += tag.strip()
                            else:
                                list_number = 1
                                # Iterate over the children of the tag to add to a predefined string only tags with
                                # readable text
                                for content in tag.contents:
                                    if tag.name in ('h2', 'h3', 'h4'):
                                        clean_description += content.text.strip() + ": "
                                    elif content.name == 'li':
                                        clean_description += str(list_number) + ") " + content.text.strip() + " "
                                        list_number += 1
                                    elif content.name in ('a', 'em'):
                                        clean_description += "\"" + content.text.strip() + "\"" + " "
                                    else:
                                        clean_description += content.text.strip() + " "

                        # Clean the final description removing double whitespaces and whitespaces before a dot
                        clean_final_description = clean_description.replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                                                   " ").replace(
                            "\n", " ").strip()

                        # Calculate the hash_id based on the title and the description
                        hash_id_data = f"{title}{clean_description}"
                        hash_id = calculate_hash(hash_id_data)

                        # Save the data into a dictionary (category name, unit name, subunit name and description)
                        support_document = {'hash_id': hash_id,
                                            'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                            'source': 'Airzone Support',
                                            'title': title,
                                            'description': clean_final_description}

                        support_document_list.append(support_document)

            logging.info("Inserting the 'Airzone Support' scraped data into a dataframe...")

            # Store the documents into the dataframe
            df = pd.DataFrame(support_document_list)

            return df

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the airzone_support_scraper function: {str(e)}")


def airzone_faq_scraper(session: Session) -> pd.DataFrame:
    logging.info("Starting the 'Airzone Control' website FAQs scraper...")

    faq_groups_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/faq-groups'
    faq_list = []
    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['hash_id', 'uploaded_date', 'source', 'title', 'description'])
    try:
        response = session.get(faq_groups_endpoint)
        response_json = response.json()

        faq_groups = [{'name': group['name'], 'reference': group['reference']} for group in
                      response_json['body']['groups']]

        for group in faq_groups:
            faq_endpoint = "https://api.airzonecloud.com/msmultimedia.pv1/faqs"
            response = session.get(faq_endpoint, params={'group': group['reference']})
            response_json = response.json()

            n_pages = response_json['body']['faqs']['last_page']

            for page in range(1, n_pages + 1):
                faq_page_endpoint = "https://api.airzonecloud.com/msmultimedia.pv1/faqs"
                response = session.get(faq_page_endpoint, params={'page': page, 'group': group['reference']})
                response_json = response.json()

                faqs = response_json['body']['faqs']['data']

                for faq in faqs:
                    faq_question = faq['question']
                    faq_raw_answer = faq['answer']

                    # In order to clean the description, we will use BeautifulSoup
                    soup = BeautifulSoup(faq_raw_answer, 'html.parser')
                    final_answer = ""
                    for tag in soup.contents:
                        if tag.name == 'a':
                            final_answer += tag.text.strip()
                        elif tag.name == 'h1' or tag.text == '':
                            continue
                        elif isinstance(tag, NavigableString):
                            final_answer += tag.strip() + " "
                        else:
                            list_number = 1
                            # Iterate over the children of the tag
                            for content in tag.contents:
                                if tag.name in ('h2', 'h3', 'h4'):
                                    final_answer += content.text.strip() + ": "
                                elif content.name == 'li':
                                    final_answer += str(list_number) + ") " + content.text.strip() + " "
                                    list_number += 1
                                elif content.name in ('a', 'em'):
                                    final_answer += "\"" + content.text.strip() + "\"" + " "
                                else:
                                    final_answer += content.text.strip() + " "

                    # Clean the final description removing double whitespaces and whitespaces before a dot
                    clean_final_answer = final_answer.replace(" .", ".").replace("   ", " ").replace("  ", " ").replace(
                        "\n", " ").replace("( ", "(").strip()

                    hash_id_data = f"{faq_question}{clean_final_answer}"
                    hash_id = calculate_hash(hash_id_data)

                    # Save the data into a dictionary (hash_id, uploaded_date, title and description)
                    faq_item = {'hash_id': hash_id,
                                'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'source': 'Airzone FAQs',
                                'title': faq_question,
                                'description': clean_final_answer}

                    faq_list.append(faq_item)

        logging.info("Inserting the 'Airzone Control' FAQs scraped data into a dataframe...")

        # Store the categories, units, subunits and descriptions in the dataframe
        df = pd.DataFrame(faq_list)

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the airzone_faq_scraper function: {str(e)}")

    return df


def airzone_downloads_scraper(db, session):
    logging.info("Starting the 'Airzone Control' website Downloads scraper...")

    groups_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/groups'
    item_list = []
    download_document_list = []
    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['hash_id', 'uploaded_date', 'source', 'title', 'description'])

    try:
        response = session.get(groups_endpoint)
        response_json = response.json()

        groups = [{'id': group['id'], 'name': group['name']} for group in response_json['body']['media_groups']]

        for group in groups:
            group_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/groups/{group['id']}"
            response = session.get(group_endpoint)
            response_json = response.json()
            group_classes = response_json['body']['media_group']['classes']
            for group_class in group_classes:
                for item in group_class['media_resources']:
                    item_name = item['name']
                    item_url = item['url']
                    # Save each item in a temporary dictionary
                    temp_item_dict = {'title': item_name,
                                      'url': item_url
                                      }
                    item_list.append(temp_item_dict)

        # Check if any document already exists in the database and remove it from the list
        collection_name = 'support'
        field_name = 'url'
        new_item_list = find_missing_documents_in_db(db, collection_name, field_name, item_list)

        # Iterate over the list of items to download the PDFs and extract the text

        for item in new_item_list:
            # Extract the text from the PDF
            chunk_dict_list = extract_text_from_pdf(item)
            download_document_list.append(chunk_dict_list)

        logging.info("Inserting the 'Airzone Control Downloads' scraped data into a dataframe...")

        # Store the categories, units, subunits and descriptions in the dataframe
        df = pd.DataFrame(item_list)

    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {str(e)}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in the airzone_downloads_scraper function: {str(e)}")

    return df


def support_scraper(session, db):
    logging.info("Starting the Support scraper...")
    try:
        support_df = airzone_support_scraper(session)
        faq_df = airzone_faq_scraper(session)
        # downloads_df = airzone_downloads_scraper(db, session)

        # Union the dataframes
        final_df = pd.concat([support_df, faq_df], ignore_index=True)

        collection = db['support']
        logging.info("Inserting the Support data into the database...")
        insert_df_into_db(collection, final_df)

    except Exception as e:
        logging.error(f"An error occurred in the support_scraper function: {str(e)}")
