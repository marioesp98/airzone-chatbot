from time import sleep

from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Initialize a Selenium WebDriver (e.g., Chrome)
driver = webdriver.Chrome()
driver.get('https://support.airzonecloud.com/#/support/ZONE_MANAGEMENT')

# Wait for a specific element to be present
# wait = WebDriverWait(driver, 10)
# element = wait.until(EC.presence_of_element_located((By.ID, 'element_id')))

WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, f"//div[@class='support__theme__unit']")))

# Get the HTML source code of the current page
html_source = driver.page_source


# Parse the HTML using BeautifulSoup
soup = BeautifulSoup(html_source, 'html.parser')


unit_subunits = soup.select('div.support__theme__unit > div.az-cell > div.az-cell__title')

for subunit in unit_subunits:
    print(subunit.get_text(strip=True))

# # Close the Selenium WebDriver
driver.quit()
