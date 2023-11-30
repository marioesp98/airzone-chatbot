from bs4 import BeautifulSoup, NavigableString
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common import TimeoutException
from support_elements import *
from support_classes import *
import re

def scrap_support_categories(driver, support_categories):
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
                print(f"Attempt {attempt + 1} failed for \"{sc}\" category, retrying... Error: {e}. ")
        else:
            # This block is executed if the inner loop completes without a 'break'
            print(f"Task failed after {MAX_RETRIES} attempts. Skipping to the next category.")
            driver.get(URL)
            continue

    return categories

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
    try:
        selenium_subunit = WebDriverWait(driver, LONG_TIMEOUT).until(
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

        description = soup.find("div", class_="support__data margin-top-half margin-bottom-triple")
        final_description = ""
        if description:
            excluded_tags = ['a']
            # Extract text excluding header tags
            for tag in description.contents:
                if tag.name in excluded_tags:
                    tag.decompose()
                elif tag.next.name in excluded_tags:
                    tag.next.decompose()
                elif tag.text == '':
                    tag.decompose()
                elif isinstance(tag, NavigableString) and tag.strip() == '':
                    tag.replace_with('')
                else:
                    list_number = 1
                    # Iterate over the children of the tag to add to a predefined string only tags with readable text
                    for content in tag.contents:
                        if tag.name in ('h2', 'h3', 'h4'):
                            final_description += content.text.strip() + ": "
                        elif content.name == 'li':
                            final_description += str(list_number) + ") " + content.text.strip() + " "
                            list_number += 1
                        elif content.name in ('a', 'em'):
                            final_description += "\"" + content.text.strip() + "\"" + " "
                        else:
                            final_description += content.text.strip() + " "


            # Clean the final description removing double whitespaces and whitespaces before a dot
            clean_final_description = final_description.replace(" .", ".").replace("   ", " ").replace("  ", " ").replace("\n", " ").strip()

            # # This is the original code
            # excluded_tags = ['h2', 'a']
            # # Extract text excluding header tags
            # filtered_description = re.sub(r'\s+', ' ', ' '.join(
            #     [f"{item.text.strip()}:" if item.name in ('h4', 'h5') else str(item.text).strip() for item in
            #      description.contents if (
            #                  item.name not in excluded_tags and item.next.name not in excluded_tags) and item.string != '']).strip())

            # Clean up text
            # filtered_description = re.sub(r'-\s*([A-Za-z0-9]+),?', r' \1', filtered_description)

            # Modify the previous regex to handle the following case: when a set of values is preceeded by a dash, remove the dash in all of them and add a comma after each one except the last one
            #filtered_description = re.sub(r'\s*-\s*([\wáéíóúüñ]+)', r'\1, ', filtered_description, flags=re.UNICODE)

        return clean_final_description

    except TimeoutException:
        print(f"Loading took too much time!")
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Wait for return button to be clickable in order to return to the main page
        return_button = WebDriverWait(driver, SHORT_TIMEOUT).until(
            EC.element_to_be_clickable((By.CLASS_NAME, RETURN_BUTTON_CLASS)))
        return_button.click()

        return clean_final_description
