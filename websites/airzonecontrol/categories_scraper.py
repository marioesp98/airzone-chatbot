import os
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString

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

support_endpoint = 'https://api.airzonecloud.com/mscrm.pv1/crm-associates/categories'
response = session.get(support_endpoint)

categories = []
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
        course_clean_description = soup.text.replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                            " ").replace(
            "\n", " ").strip()

        # Save the data into a dictionary (id, profile, mode, title, description)
        course_dict = {'id': course_id, 'category': course_category, 'profile': course_profile, 'mode': course_mode, 'title': course_title,
                       'description': course_clean_description}
        course_list.append(course_dict)

    # Create a dataframe to store the data in the course list
    df = pd.DataFrame(columns=['ID', 'Category', 'Profile', 'Mode', 'Title', 'Description'])

    # Store the categories, units, subunits and descriptions in the dataframe
    for course in course_list:
        df = df._append({'ID': course['id'], 'Category': course['category'], 'Profile': course['profile'], 'Mode': course['mode'],
                         'Title': course['title'], 'Description': course['description']}, ignore_index=True)

    # Save the dataframe to a csv file
    df.to_csv('results/airzone_academia_courses.csv', mode='a', index=False)