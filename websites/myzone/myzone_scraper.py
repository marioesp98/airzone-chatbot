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
# options.add_argument("--headless")

driver = webdriver.Chrome(options=options)
driver.maximize_window()
#driver.set_window_size(1920, 1080)  # Adjust the size as needed

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

# scraped_general_information = []
#
# # First, some general data from the main page will be retrieved before moving on to the categories pages
# # 1) Extract social network information
# scraped_social_networks = scrap_social_networks(driver, soup)
# scraped_general_information.append(scraped_social_networks)
#
# # 2) Extract general information about Airzone
# scraped_contact_information = scrap_general_information(driver, soup)
# scraped_general_information.append(scraped_contact_information)
#
# # 2) Extract information regarding manufacturing times
# scraped_tracking_information = scrap_myzone_tracking(driver)
# scraped_general_information.append(scraped_tracking_information)
#
# # 3) Extract information from the footer
# scraped_footer_categories = scrap_myzone_footer(driver, soup)
# scraped_general_information.append(scraped_footer_categories)

# 4) Extract tools information
# scraped_tools_information = scrap_myzone_tools(driver)
# scraped_general_information.append(scraped_tools_information)
# # Next, we will start scrapping Airzone's products
# scraped_products, scraped_faq = scrap_myzone_products(driver)
# scraped_general_information.append(scraped_faq)

# Scrap Airzone compatibility tool
scraped_compatibility = scrap_airzone_compatibility(driver)

# flat_general_information_list = [item for sublist in scraped_general_information for item in sublist]
#
# # Create a dataframe to store the general information data
# df = pd.DataFrame(columns=['Category', 'Unit', 'Subunit', 'Description'])
#
# # Store the categories, units, subunits and descriptions in the dataframe
# for category in flat_general_information_list:
#     for unit in category.units:
#         for subunit in unit.subunits:
#             df = df._append({'Category': category.name, 'Unit': unit.name, 'Subunit': subunit.name,
#                             'Description': subunit.description}, ignore_index=True)
#
# # Append dataframe rows to csv
# df.to_csv('results/myzone_general_information.csv', mode='a', index=False)
#
#
# # Create a dataframe to store the data
# df = pd.DataFrame(columns=['Category', 'Unit', 'Subunit', 'Product', 'P_N', 'EAN', 'Description'])
#
# # Store the categories, units, subunits and descriptions in the dataframe
# for category in scraped_products:
#     for unit in category.units:
#         for subunit in unit.subunits:
#             for product in subunit.products:
#                 df = df._append({'Category': category.name, 'Unit': unit.name, 'Subunit': subunit.name,
#                                 'Product': product.name, 'P_N': product.p_n, 'EAN': product.ean,
#                                 'Description': product.description}, ignore_index=True)
#
# # Save the dataframe to a csv file
# df.to_csv('results/myzone_products.csv', index=False)

# Close the browser window
driver.quit()