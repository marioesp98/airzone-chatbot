from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common import TimeoutException
from support_elements import *
from support_classes import *
import re


def scrap_support_units(driver):
    units = []
    # Wait for the units to be present
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, f"//div[@class='support__theme__unit']")))

    # Get the HTML source code of the category page
    html_source = driver.page_source

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    category_units = [su.get_text(strip=True) for su in soup.select('div.support__theme__unit > '
                                                                    'div.az-cell > div.az-cell__title')]
    for cu in category_units:
        unit = Unit(cu)
        # In order to handle errors, the process will be retried a maximum of N times
        for attempt in range(MAX_RETRIES):
            try:
                subunits = scrap_support_subunits(driver, soup, unit)
                for subunit in subunits:
                    unit.add_subunit(subunit)
                units.append(unit)
                print(f"Attempt {attempt + 1} succeeded for \"{cu}\" unit.")
                break
            except TimeoutException as e:
                print(f"Attempt {attempt + 1} timed out. Retrying...")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for \"{cu}\" unit: {e}.")
        else:
            # This block is executed if the inner loop completes without a 'break'
            print(f"Task failed after {MAX_RETRIES} attempts. Skipping to the next category.")
            driver.get(URL)
            continue
    # Wait for return button to be clickable in order to return to the main page
    return_button = WebDriverWait(driver, SHORT_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, RETURN_BUTTON_CLASS)))
    return_button.click()
    return units


def scrap_support_subunits(driver, soup, unit):
    subunits = []
    # We are inside the corresponding support unit, so let's scrap the subunits and descriptions
    unit_subunits = soup.select(f'.support__theme__unit:contains("{unit.name}") .support__theme__subUnit '
                                f'.az-cell.padding-left.az-cell--link .az-cell__title')
    for unit_subunit in unit_subunits:
        subunit_description = scrap_subunit_description(driver, unit_subunit)
        subunit = Subunit(unit_subunit.get_text(strip=True))
        subunit.add_description(subunit_description)
        subunits.append(subunit)
    return subunits


def scrap_subunit_description(driver, subunit):
    filtered_description = None
    try:
        selenium_subunit = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'az-cell padding-left az-cell--link') "
                                                      f"and .//div[@class='az-cell__title' and contains(text(), "
                                                      f"'{subunit.get_text(strip=True)}')]]")))

        selenium_subunit.click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[contains(text(), '{subunit.get_text(strip=True)}')]")))

        # Get the HTML source code of the subunit page
        html_source = driver.page_source

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(html_source, 'html.parser')

        description = soup.find("div", class_="support__data margin-top-half margin-bottom-triple").get_text(strip=True).strip()
        description_header = soup.find("h2").get_text(strip=True)

        # Filter the description to remove the header
        filtered_description = description.lstrip(description_header).replace(".", ". ")

        return filtered_description

    except TimeoutException:
        print(f"Loading took too much time!")

    finally:
        # Wait for return button to be clickable in order to return to the main page
        return_button = WebDriverWait(driver, SHORT_TIMEOUT).until(
            EC.element_to_be_clickable((By.CLASS_NAME, RETURN_BUTTON_CLASS)))
        return_button.click()

        return filtered_description
