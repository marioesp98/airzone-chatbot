import hashlib
import time
import pandas as pd
from bs4 import BeautifulSoup, NavigableString

from resources.general_functions import calculate_hash, insert_df_into_db


def airzone_support_scraper(session):
    support_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/digital-doc/books'
    support_items = []
    categories = []
    target_iso = 'MU_AZCLOUD'

    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'support_source', 'category', 'unit', 'subunit',
                 'type', 'description'])

    response = session.get(support_endpoint)
    response_json = response.json()

    digital_books_data = response_json['body']['digital_books']['data']

    category_list = next((item for item in digital_books_data if item.get('az_iso') == target_iso), None)

    # If the categories are found in the response, extract the 'name' values for each list element in 'digital_sections'
    if category_list and 'digital_sections' in category_list:
        for category in category_list['digital_sections']:
            # Add the category name and az_iso into a dictionary
            categories.append({'name': category['name'], 'az_iso': category['az_iso']})

        for category in categories:
            category_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/digital-doc/sections/{category['az_iso']}"
            response = session.get(category_endpoint)
            response_json = response.json()
            units = response_json['body']['digital_section']['digital_subsections']

            for unit in units:
                unit_name = unit['name']
                for subunit in unit['digital_contents']:
                    subunit_id = subunit['id']
                    subunit_name = subunit['name']
                    subunit_raw_description = subunit['description']

                    # In order to clean the description, we will use BeautifulSoup
                    soup = BeautifulSoup(subunit_raw_description, 'html.parser')
                    final_description = ""
                    excluded_tags = ['a']
                    for tag in soup.contents:
                        if tag.name in excluded_tags:
                            tag.decompose()
                        elif tag.next.name in excluded_tags:
                            tag.next.decompose()
                        elif tag.text == '':
                            tag.decompose()
                        elif isinstance(tag, NavigableString):
                            final_description += tag.strip()
                        else:
                            list_number = 1
                            # Iterate over the children of the tag to add to a predefined string only tags with readable text
                            for content in tag.contents:
                                if tag.name in ('h2', 'h3', 'h4'):
                                    final_description += content.text.strip() + ": "
                                elif content.name == 'li':
                                    final_description += str(list_number) + ") " + content.text.strip() + " "
                                    list_number += 1
                                elif content.name in ('a', 'em'):
                                    final_description += "\"" + content.text.strip() + "\"" + " "
                                else:
                                    final_description += content.text.strip() + " "

                    # Clean the final description removing double whitespaces and whitespaces before a dot
                    clean_final_description = final_description.replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                                               " ").replace(
                        "\n", " ").strip()

                    # Include primary_hash_id and mod_hash_id
                    primary_hash_id_data = f"{category['name']}{unit_name}{subunit_name}"
                    primary_hash_id = calculate_hash(primary_hash_id_data)

                    mod_hash_id_data = clean_final_description
                    mod_hash_id = calculate_hash(mod_hash_id_data)

                    # Save the data into a dictionary (category name, unit name, subunit name and description)
                    support_item = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                                    'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'support_source': 'Airzone Support',
                                    'category': category['name'], 'unit': unit_name,
                                    'subunit': subunit_name,
                                    'type': '',
                                    'description': clean_final_description}

                    support_items.append(support_item)

        # Store the categories, units, subunits and descriptions in the dataframe
        for support_item in support_items:
            df = df._append(
                {'primary_hash_id': support_item['primary_hash_id'], 'mod_hash_id': support_item['mod_hash_id'],
                 'uploaded_date': support_item['uploaded_date'],
                 'support_source': support_item['support_source'],
                 'category': support_item['category'], 'unit': support_item['unit'],
                 'subunit': support_item['subunit'],
                 'type': support_item['type'],
                 'description': support_item['description']}, ignore_index=True)

    return df


def airzone_faq_scraper(session):
    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'support_source', 'category', 'unit', 'subunit', 'type',
                 'description'])

    faq_groups_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/faq-groups'
    faq_items = []

    response = session.get(faq_groups_endpoint)
    response_json = response.json()

    faq_groups = [{'name': group['name'], 'reference': group['reference']} for group in response_json['body']['groups']]

    for group in faq_groups:
        faq_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/faqs?group={group['reference']}"
        response = session.get(faq_endpoint)
        response_json = response.json()

        n_pages = response_json['body']['faqs']['last_page']

        for page in range(1, n_pages + 1):
            faq_page_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/faqs?page={page}&group={group['reference']}"
            response = session.get(faq_page_endpoint)
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

                # Include primary_hash_id and mod_hash_id
                primary_hash_id_data = f"{group['name']}{faq_question}"
                primary_hash_id = calculate_hash(primary_hash_id_data)

                mod_hash_id_data = clean_final_answer
                mod_hash_id = calculate_hash(mod_hash_id_data)

                # Save the data into a dictionary (primary_hash_id, mod_hash_id, uploaded_date, category name, unit name, subunit name and description)
                faq_item = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                            'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                            'support_source': 'Airzone FAQs',
                            'category': group['name'], 'unit': 'FAQ',
                            'subunit': faq_question,
                            'type': '',
                            'description': clean_final_answer}

                faq_items.append(faq_item)

    # Store the categories, units, subunits and descriptions in the dataframe
    for faq_item in faq_items:
        df = df._append(
            {'primary_hash_id': faq_item['primary_hash_id'], 'mod_hash_id': faq_item['mod_hash_id'],
             'uploaded_date': faq_item['uploaded_date'],
             'support_source': faq_item['support_source'],
             'category': faq_item['category'], 'unit': faq_item['unit'],
             'subunit': faq_item['subunit'],
             'type': faq_item['type'],
             'description': faq_item['description']}, ignore_index=True)

    return df


def airzone_downloads_scraper(session):
    # Create a dataframe to store the data
    df = pd.DataFrame(
        columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'support_source', 'category', 'unit', 'subunit', 'type',
                 'description'])

    groups_endpoint = 'https://api.airzonecloud.com/msmultimedia.pv1/groups'
    items = []

    response = session.get(groups_endpoint)
    response_json = response.json()

    groups = [{'id': group['id'], 'name': group['name']} for group in response_json['body']['media_groups']]

    for group in groups:
        group_name = group['name']
        group_endpoint = f"https://api.airzonecloud.com/msmultimedia.pv1/groups/{group['id']}"
        response = session.get(group_endpoint)
        response_json = response.json()
        group_classes = response_json['body']['media_group']['classes']
        for group_class in group_classes:
            class_name = group_class['name']
            for item in group_class['media_resources']:
                item_name = item['name']
                item_type = item['media_type_name']
                item_url = item['url']

                # Include primary_hash_id and mod_hash_id
                primary_hash_id_data = f"{group_name}{class_name}{item_name}{item_type}"
                primary_hash_id = calculate_hash(primary_hash_id_data)

                mod_hash_id_data = item_url
                mod_hash_id = calculate_hash(mod_hash_id_data)

                # Save the data into a dictionary (primary_hash_id, mod_hash_id, uploaded_date, category name,
                # unit name, subunit name and description)
                item_dict = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                             'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                             'support_source': 'Airzone Downloads',
                             'category': group_name, 'unit': class_name,
                             'subunit': item_name, 'type': item_type,
                             'url': item_url}

                items.append(item_dict)

    # Store the categories, units, subunits and descriptions in the dataframe
    for item in items:
        df = df._append(
            {'primary_hash_id': item['primary_hash_id'], 'mod_hash_id': item['mod_hash_id'],
             'uploaded_date': item['uploaded_date'],
             'support_source': item['support_source'],
             'category': item['category'], 'unit': item['unit'],
             'subunit': item['subunit'],
             'type': item['type'],
             'description': item['url']}, ignore_index=True)

    return df


def support_scraper(session):
    support_df = airzone_support_scraper(session)
    faq_df = airzone_faq_scraper(session)
    downloads_df = airzone_downloads_scraper(session)

    # Union the dataframes
    final_df = pd.concat([support_df, faq_df, downloads_df], ignore_index=True)

    query = "INSERT INTO support (primary_hash_id, mod_hash_id, uploaded_date, support_source, category, unit, subunit, " \
            "type, description) VALUES (%s, %s, " \
            "%s, %s, %s, %s, %s, %s, %s)"

    insert_df_into_db(final_df, query, 'support')

