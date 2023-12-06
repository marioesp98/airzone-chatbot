from resources.variables import *
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from websites.support.support_functions import scrap_support_categories
import pandas as pd
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=options)
driver.maximize_window()

driver.get(SUPPORT_URL)

try:
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, SUPPORT_BUTTON_CLASS)))
    print("Page is ready!")
except TimeoutException:
    print("Loading took too much time!")

# Get the page source after waiting
html_source = driver.page_source

# Parse the HTML using BeautifulSoup
soup = BeautifulSoup(html_source, 'html.parser')

# Extract and store all the support options from the main page
support_categories = [sc.get_text(strip=True) for sc in soup.find_all('div', class_=SUPPORT_BUTTON_CLASS)]

# Iterate over each support category to retrieve all data related to it
categories = scrap_support_categories(driver, support_categories)

# Create a dataframe to store the data
df = pd.DataFrame(columns=['Category', 'Unit', 'Subunit', 'Description'])

# Store the categories, units, subunits and descriptions in the dataframe
for category in categories:
    for unit in category.units:
        for subunit in unit.subunits:
            df = df._append({'Category': category.name, 'Unit': unit.name, 'Subunit': subunit.name,
                            'Description': subunit.description}, ignore_index=True)

# Now it's time to add the hash key and the mod hash key to the dataframe
df['primary_hash_key'] = ''
df['mod_hash_key'] = ''

# Iterate over each row of the dataframe to add the SHA-256 hash keys
for index, row in df.iterrows():
    # Get the SHA-256 hash key for the primary key
    primary_string = row['Category'] + row['Unit'] + row['Subunit']
    mod_string = row['Description']

    primary_hash_key = get_sha256_hash_key(row['Category'], row['Unit'], row['Subunit'])
    # Get the SHA-256 hash key for the mod key
    mod_hash_key = get_sha256_hash_key(row['Category'], row['Unit'], row['Subunit'], row['Description'])
    # Add the hash keys to the dataframe
    df.loc[index, 'primary_hash_key'] = primary_hash_key
    df.loc[index, 'mod_hash_key'] = mod_hash_key


# Save the dataframe to a csv file
df.to_csv('results/support_scrapped_data.csv', index=False)

# Close the browser window
driver.quit()

# # Extract text from 'p' elements
# text_elements_p = [p.get_text(strip=True) for p in soup.find_all(['p', 'blockquote'])]
#
# # Extract text from 'a' elements
# text_elements_a = [a.get_text(strip=True) for a in soup.find_all('a')]
#
# # Discard text that appears in both 'p' and 'a' elements
# filtered_text_elements = [text for text in text_elements_p if text not in text_elements_a]
#
# # Print the extracted text
# for text in filtered_text_elements:
#     print(text)
