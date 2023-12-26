import hashlib
import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString
from resources.general_functions import insert_df_into_db

api_key = os.environ.get('AIRZONE_API_KEY')

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'apikey': api_key,
    'app-locale': 'es',
    'app-market': 'ib'
}
session = requests.Session()
session.headers.update(headers)

support_endpoint = 'https://api.airzonecloud.com/msacademy.pv1/courses/'
response = session.get(support_endpoint)

support_items = []
response_json = response.json()
courses = response_json['body']['courses']

# If the categories are found in the response, extract the 'name' values for each list element in 'digital_sections'
course_list = []
if courses:
    for course in courses:
        course_id = course['uuid']
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
        course_clean_description = soup.text.lstrip('Webinar').replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                            " ").replace(
            "\n", " ").strip()

        # Include primary_hash_id and mod_hash_id
        primary_hash_id_data = f"{course_category}{course_profile}{course_mode}{course_title}".encode('utf-8')
        primary_hash_id = hashlib.sha256(primary_hash_id_data).hexdigest()

        mod_hash_id_data = course_clean_description.encode('utf-8')
        mod_hash_id = hashlib.sha256(mod_hash_id_data).hexdigest()

        # Save the data into a dictionary (id, profile, mode, title, description)
        course_dict = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id, 'category': course_category, 'profile': course_profile, 'mode': course_mode, 'title': course_title,
                       'description': course_clean_description}
        course_list.append(course_dict)

    # Create a dataframe to store the data in the course list
    df = pd.DataFrame(columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'category', 'profile', 'mode', 'title', 'description'])

    # Store the categories, units, subunits and descriptions in the dataframe
    for course in course_list:
        df = df._append({'primary_hash_id': course['primary_hash_id'], 'mod_hash_id': course['mod_hash_id'], 'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'), 'category': course['category'], 'profile': course['profile'], 'mode': course['mode'],
                         'title': course['title'], 'description': course['description']}, ignore_index=True)


    query = "INSERT INTO academia (primary_hash_id, mod_hash_id, uploaded_date, category, profile, mode, title, " \
            "description) VALUES (%s, %s, " \
            "%s, %s, %s, %s, %s, %s)"

    insert_df_into_db(df, query, 'academia')