import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString

from resources.general_functions import calculate_hash, insert_df_into_db


def general_information_scraper(session):
    translations_endpoint = 'https://api.airzonecloud.com/msairpress.pv1/translations/es-ES'
    response = session.get(translations_endpoint)

    general_information_list = []
    general_information_dict_list = []
    # First, get 'About Airzone' information
    about_airzone = response.json()['body']['about']
    # Check if there is any element starting with 'paragraph'
    about_airzone_description = ''
    if about_airzone:
        for key, value in about_airzone.items():
            if key.startswith('paragraph'):
                about_airzone_description += value

        clean_about_airzone_description = re.sub(r'\s+', ' ', about_airzone_description)

        # Create a tuple with response.json['body']['altImage'] and the description
        about_airzone_tuple = (about_airzone['altImage'], clean_about_airzone_description)
        general_information_list.append(about_airzone_tuple)

    # Next, get contact information about Airzone
    market_endpoint = 'https://api.airzonecloud.com/msmarket.pv1/markets/market'
    response = session.get(market_endpoint)

    contact_information = response.json()['body']['market']

    commercial_contact = ('Contacto comercial', contact_information['commercial_contact'])
    general_information_list.append(commercial_contact)
    bank_name = ('Banco', contact_information['bank_name'])
    general_information_list.append(bank_name)
    bank_account = ('Cuenta bancaria', contact_information['bank_account'])
    general_information_list.append(bank_account)
    address = ('Dirección', contact_information['altra_company']['address'])
    general_information_list.append(address)
    cif = ('CIF', contact_information['altra_company']['cif'])
    general_information_list.append(cif)
    city = ('Ciudad', contact_information['altra_company']['city'])
    general_information_list.append(city)
    company_name = ('Compañía', contact_information['altra_company']['name'])
    general_information_list.append(company_name)
    phone = ('Teléfono', contact_information['altra_company']['phone'])
    general_information_list.append(phone)
    postal_code = ('Código postal', contact_information['altra_company']['postal_code'])
    general_information_list.append(postal_code)
    web_url = ('WEB', contact_information['altra_company']['web'])
    general_information_list.append(web_url)

    # Add 'Contact Information' at the beginning of each tuple
    general_information_list = [('Contacto', item[0], item[1]) for item in general_information_list]

    # Next, get the footer sections from MyZone website
    endpoints = (('Política de privacidad', 'https://myzone.airzone.es/politica-privacidad'),
                 ('Condiciones de uso', 'https://myzone.airzone.es/condiciones-uso'),
                 ('Aviso legal', 'https://myzone.airzone.es/aviso-legal'),
                 ('Política de cookies', 'https://myzone.airzone.es/politica-cookies'))

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
                general_information_list.append((endpoint[0], section_name, clean_description))

    for item in general_information_list:
        # Include primary_hash_id and mod_hash_id
        primary_hash_id_data = f"{item[0]}{item[1]}"
        primary_hash_id = calculate_hash(primary_hash_id_data)

        mod_hash_id_data = item[2]
        mod_hash_id = calculate_hash(mod_hash_id_data)

        item_dict = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                     'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'), 'support_source': 'General Information',
                     'category': 'General Information',
                     'unit': item[0], 'subunit': item[1],
                     'type': '', 'description': item[2]}

        general_information_dict_list.append(item_dict)

    # Create a dataframe to store the data in the general information list
    df = pd.DataFrame(
        columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'support_source', 'category', 'unit', 'subunit',
                 'type', 'description'])

    # Store the categories, units, subunits and descriptions in the dataframe
    for item in general_information_dict_list:
        df = df._append(item, ignore_index=True)
    pass
    query = "INSERT INTO support (primary_hash_id, mod_hash_id, uploaded_date, support_source, category, unit, " \
            "subunit, type, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    insert_df_into_db(df, query, 'support')



