import pandas as pd
from resources.variables import *
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from myzone_functions import *


options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=options)
driver.maximize_window()

driver.get(MYZONE_URL)

try:
    cookies_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, MYZONE_COOKIE_ID)))
    cookies_element.click()
    print("Page is ready!")
except TimeoutException:
    print("Loading took too much time!")

# Get the page source after waiting
html_source = driver.page_source

# Parse the HTML using BeautifulSoup
soup = BeautifulSoup(html_source, 'html.parser')

scrapped_general_information = []

# First, some general data from the main page will be retrieved before moving on to the categories pages
# 1) Extract social network information
# scrapped_social_networks = scrap_social_networks(driver, soup)
# scrapped_general_information.append(scrapped_social_networks)
#
# # 2) Extract general information about Airzone
# scrapped_general_information = scrap_general_information(driver, soup)
# scrapped_general_information.append(scrapped_general_information)
#
# # 2) Extract information from the footer
# scrapped_footer_categories = scrap_myzone_footer(driver, soup)
# scrapped_general_information.append(scrapped_footer_categories)

# Next, we will start scrapping Airzone's products
scrapped_products = scrap_myzone_products(driver)

# Create a dataframe to store the data
df = pd.DataFrame(columns=['Category', 'Unit', 'Subunit', 'Product', 'P_N', 'EAN', 'Description'])

# Store the categories, units, subunits and descriptions in the dataframe
for category in scrapped_products:
    for unit in category.units:
        for subunit in unit.subunits:
            for product in subunit.products:
                df = df._append({'Category': category.name, 'Unit': unit.name, 'Subunit': subunit.name,
                                'Product': product.name, 'P_N': product.p_n, 'EAN': product.ean,
                                'Description': product.description}, ignore_index=True)

# Save the dataframe to a csv file
df.to_csv('results/myzone_products.csv', index=False)

# Close the browser window
driver.quit()