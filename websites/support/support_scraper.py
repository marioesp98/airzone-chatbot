import time
from time import sleep
from support_elements import *
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from websites.support.support_classes import *
from websites.support.support_functions import scrap_support_units

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=options)
driver.maximize_window()

driver.get(URL)

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

categories = []
for sc in support_categories:
    # In order to handle errors, the process will be retried a maximum of 3 times
    for attempt in range(MAX_RETRIES):
        try:
            category = Category(sc)
            support_button = WebDriverWait(driver, SHORT_TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, f"//div[text()='{sc}']")))
            support_button.click()
            # Wait for loading icon to disappear before continuing
            try:
                WebDriverWait(driver, LONG_TIMEOUT).until_not(
                    EC.presence_of_element_located((By.CLASS_NAME, LOADING_ICON_CLASS)))

            except TimeoutException:
                print(f"Loading icon was not detected. Continuing...")
                pass

            # Now we are inside the corresponding support category, so let's scrap the units, subunits and descriptions
            units = scrap_support_units(driver)
            # And finally we add the list of units to the category and append it to the list of categories
            category.add_units(units)
            categories.append(category)

            # Wait for return button to be clickable in order to return to the main page
            return_button = WebDriverWait(driver, SHORT_TIMEOUT).until(
                EC.element_to_be_clickable((By.CLASS_NAME, RETURN_BUTTON_CLASS)))
            return_button.click()
            print(f"Attempt {attempt + 1} succeeded for \"{sc}\" category.")
            break
        except TimeoutException as e:
            print(f"Attempt {attempt + 1} timed out. Retrying...")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for \"{sc}\" category: {e}. Retrying...")
    else:
        # This block is executed if the inner loop completes without a 'break'
        print(f"Task failed after {MAX_RETRIES} attempts. Skipping to the next category.")
        driver.get(URL)
        continue

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
