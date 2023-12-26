import os
import pandas as pd
import requests
import hashlib

from resources.general_functions import insert_df_into_db
from websites.support.support_API_functions import *

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

support_df = airzone_support_scraper(session)
faq_df = airzone_faq_scraper(session)

# Union the dataframes
final_df = pd.concat([support_df, faq_df], ignore_index=True)

query = "INSERT INTO support (primary_hash_id, mod_hash_id, uploaded_date, category, unit, subunit, description) VALUES (%s, %s, " \
        "%s, %s, %s, %s, %s)"

insert_df_into_db(final_df, query, 'support')
# ----------------------------------------------------------------------------------------------------------------------
#
# # Now it's time to add the hash key and the mod hash key to the dataframe
# df['primary_hash_key'] = ''
# df['mod_hash_key'] = ''
#
# # Iterate over each row of the dataframe to add the SHA-256 hash keys
# for index, row in df.iterrows():
#     # Get the SHA-256 hash key for the primary key
#     primary_string = row['Category'] + row['Unit'] + row['Subunit']
#     mod_string = row['Description']
#
#     primary_hash_key = get_sha256_hash_key(row['Category'], row['Unit'], row['Subunit'])
#     # Get the SHA-256 hash key for the mod key
#     mod_hash_key = get_sha256_hash_key(row['Category'], row['Unit'], row['Subunit'], row['Description'])
#     # Add the hash keys to the dataframe
#     df.loc[index, 'primary_hash_key'] = primary_hash_key
#     df.loc[index, 'mod_hash_key'] = mod_hash_key
#

