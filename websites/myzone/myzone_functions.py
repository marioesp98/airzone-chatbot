import html
import re
import reprlib
from time import sleep

from bs4 import BeautifulSoup, NavigableString
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common import TimeoutException, NoSuchElementException
from resources.variables import *
from resources.model_classes import *
from selenium.webdriver.common.action_chains import ActionChains


def scrap_social_networks(driver, soup):
    facebook = ('facebook', soup.select_one("a[href*=facebook]")['href'])
    twitter = ('twitter', soup.select_one("a[href*=twitter]")['href'])
    youtube = ('youtube', soup.select_one("a[href*=youtube]")['href'])
    linkedin = ('linkedin', soup.select_one("a[href*=linkedin]")['href'])
    instagram = ('instagram', soup.select_one("a[href*=instagram]")['href'])

    # Create a category, unit, subunit and description for each social network
    social_networks = []
    for social_network in [facebook, twitter, youtube, linkedin, instagram]:
        social_networks.append(Category('General Information', Unit('Social Networks', Subunit(social_network[0], social_network[1]))))

    return social_networks


def scrap_general_information(driver, soup):
    # Get
    who_are_we = ('Sobre Nosotros', soup.select_one('div.col-sm-6.inner > p').text.strip())
    address = ('Dirección', soup.select_one('i.icon-location.contact').next_sibling.strip())
    phone_number = ('Teléfono', soup.select_one('i.icon-mobile.contact').next_sibling.strip())

    # Create a category, unit, subunit and description for each general information
    general_information = []
    for general_info in [who_are_we, address, phone_number]:
        general_information.append(Category('General Information', Unit(general_info[0], Subunit('', general_info[1]))))

    return general_information

def scrap_myzone_tracking(driver):
    tracking_information = []
    tracking_button = WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href='/plazos']")))
    tracking_button.click()

    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.presence_of_element_located((By.XPATH, "//div[@class='col-sm-12 inner-left-xs inner-top-xs']")))
    # Wait until the numbers are calculated in the JS functions and displayed
    sleep(2)
    # Get the page source of the footer category page
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')


    manufacturing_sections = [e.get_text(strip=True).split(':')[0].strip() for e in soup.select('div.col-sm-12.inner-left-xs.inner-top-xs > ul > li > label')]
    manufacturing_times = [e.get_text(strip=True) for e in soup.select('div.col-sm-12.inner-left-xs.inner-top-xs > ul > li')]

    for idx, (ms, mt) in enumerate(zip(manufacturing_sections, manufacturing_times)):
        manufacturing_times[idx] = mt.replace(ms, '').replace(':', '').strip()
        tracking_information.append(Category('General Information', Unit('Manufacturing times', Subunit(manufacturing_sections[idx], manufacturing_times[idx]))))

    return tracking_information



def scrap_myzone_footer(driver, soup):
    scraped_footer_categories = []
    # Go to the bottom of the page
    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)

    # Using BeautifulSoup, get all the list elements inside class 'footer-menu pull-right'
    footer_elements = [e.get_text(strip=True) for e in soup.select('ul.footer-menu.pull-right li')]

    footer_elements_to_scrap = ['Política de privacidad', 'Condiciones de uso', 'Aviso legal', 'Politica de cookies']
    for element in footer_elements_to_scrap:
        if element in footer_elements:
            # Click on the category to go to its page
            WebDriverWait(driver, LONG_TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, f"//a[text()='{element}']"))).click()
            # Wait for page to fully load
            WebDriverWait(driver, LONG_TIMEOUT).until(
                EC.presence_of_element_located((By.CLASS_NAME, MYZONE_ICON_CLASS)))

            # Get the page source of the footer category page
            html_source = driver.page_source
            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(html_source, 'html.parser')

            units = soup.select('div.col-md-10 > div.row')

            category = Category('General Information')
            for element_unit in units:
                description = ''
                unit_name = None
                for tag in element_unit.contents:
                    if tag.name == 'h5':
                        unit_name = tag.text.strip()
                    elif tag.name == 'p':
                        description += tag.text.strip() + " "
                    elif tag.name == 'ul' or tag.name == 'ol':
                        list_number = 1
                        for li in tag.contents:
                            if li.name == 'li':
                                description += str(list_number) + ") " + li.text.strip() + " "
                                list_number += 1

                if unit_name is not None:
                    subunit = Subunit(unit_name.capitalize(), description)
                    unit = Unit(element, subunit)
                    category.add_unit(unit)

            scraped_footer_categories.append(category)

    return scraped_footer_categories


def scrap_myzone_products(driver):
    scraped_categories = []
    scraped_categories_faq = []
    # Go to the top of the page
    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.HOME)
    # Click on the 'Product' button
    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href='/productos/']"))).click()
    # Wait until page is fully loaded
    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.presence_of_element_located((By.XPATH, "//h2[text()='Preguntas frecuentes']")))

    # Get the page source of the Product page
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    # Start scraping each category, unit, subunit, item and its description
    categories = [c.get_text(strip=True) for c in soup.select('h3.sidelines.text-center')]

    for idx, category in enumerate(categories):
        scraped_category = Category(category)
        category_element = driver.find_element(By.XPATH,
                                               f"//h3[@class='sidelines text-center' and span[text()='{category}']]")
        ul_element = category_element.find_element(By.XPATH, 'following-sibling::ul[@class="metismenu"]')
        li_elements = ul_element.find_elements(By.XPATH, f".//li[@class='categoria']/a/span[@class='sidebar-nav-item']")

        units = [u.text.strip() for u in li_elements]
        # Scrap each of the units inside the category
        scraped_units, scraped_units_faq = scrap_product_units(driver, category, units)
        scraped_categories_faq.append(scraped_units_faq)
        # Add the units into the category object
        scraped_category.add_units(scraped_units)
        scraped_categories.append(scraped_category)

    flat_scraped_faq = [item for sublist in scraped_categories_faq for item in sublist]

    return scraped_categories, flat_scraped_faq


def scrap_product_units(driver, category, units):
    scraped_units = []
    scraped_units_faq = []
    for idx, unit in enumerate(units):
        scraped_unit = Unit(unit)
        category_element = driver.find_element(By.XPATH,
                                               f"//h3[@class='sidelines text-center' and span[text()='{category}']]")
        ul_element = category_element.find_element(By.XPATH, 'following-sibling::ul[@class="metismenu"]')
        li_element = ul_element.find_element(By.XPATH, f".//li[@class='categoria']/a/span[contains(text(), '{unit}')]")

        if li_element is not None:
            ActionChains(driver).move_to_element(li_element).perform()
            li_element.click()
            WebDriverWait(driver, LONG_TIMEOUT).until(EC.presence_of_element_located(
                (By.XPATH, "//ul[@class='collapse in']")))

            # Get the page source again to locate the unit with its subunits visible
            html_source = driver.page_source
            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(html_source, 'html.parser')

            # Get all the subunits inside the unit
            subunits = [u.get_text(strip=True).split('(')[0].strip() for u in soup.select('ul.collapse.in > li')]
            if 'Ver todos' in subunits:
                subunits.remove('Ver todos')

            # Scrap all the subunits inside each unit
            scraped_subunits, scraped_subunits_faq = scrap_product_subunits(driver, subunits)
            scraped_units_faq.append(scraped_subunits_faq)
            # Add the subunits into the unit object
            scraped_unit.add_subunits(scraped_subunits)
            scraped_units.append(scraped_unit)

            # Close the subunits list before continuing with the next unit
            category_element = driver.find_element(By.XPATH,
                                                   f"//h3[@class='sidelines text-center' and span[text()='{category}']]")
            ul_element = category_element.find_element(By.XPATH, 'following-sibling::ul[@class="metismenu"]')
            li_element = ul_element.find_element(By.XPATH, f'.//li[@class="categoria active"]/a')
            actions.move_to_element(li_element).perform()
            li_element.click()
            WebDriverWait(driver, LONG_TIMEOUT).until_not(EC.presence_of_element_located(
                (By.XPATH, "//ul[@class='collapse in']")))
            WebDriverWait(driver, LONG_TIMEOUT).until_not(EC.presence_of_element_located(
                (By.XPATH, "//ul[@class='collapsing']")))

    flat_scraped_faq = [item for sublist in scraped_units_faq for item in sublist]

    return scraped_units, flat_scraped_faq


def scrap_product_subunits(driver, subunits):
    scraped_subunits = []
    scraped_subunits_faq = []
    for subunit in subunits:
        #try:
            scraped_subunit = Subunit(subunit)
            subunit_element = WebDriverWait(driver, LONG_TIMEOUT).until(EC.element_to_be_clickable(
                (By.XPATH, f"//ul[@class='collapse in']/li/a[contains(text(), '{subunit}')]")))
            # Click subunit to go to its page if it was found
            if subunit_element is not None:
                ActionChains(driver).move_to_element(subunit_element).perform()
                subunit_element.click()
                # Now it is time to scrap the subunit products
                WebDriverWait(driver, LONG_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, "//ul[@class='products_list']")))

                # Get the page source again to locate the unit with its subunits visible
                html_source = driver.page_source
                # Parse the HTML using BeautifulSoup
                soup = BeautifulSoup(html_source, 'html.parser')

                products = [p.get_text(strip=True) for p in
                            soup.select('div.col-xs-8.col-sm-9.col-lg-8 > h2 > a')]

                scraped_products, scraped_faq = scrap_subunit_products(driver, products)
                scraped_subunits_faq.append(scraped_faq)
                scraped_subunit.add_products(scraped_products)
                scraped_subunits.append(scraped_subunit)

                # Hide the banner to avoid it from overlapping any buttons
                try:
                    banner = driver.find_element(By.XPATH,
                                                 "//div[@class='yamm']")
                    driver.execute_script("arguments[0].setAttribute('style','visibility:hidden')", banner)
                except NoSuchElementException:
                    print("No banner found, continuing...")
        # except:
        #     print("No subunit found, continuing...")
        #     continue

    flat_scraped_faq = [item for sublist in scraped_subunits_faq for item in sublist]

    return scraped_subunits, flat_scraped_faq


def scrap_subunit_products(driver, products):
    scraped_products = []
    scraped_faq = []
    for product in products:
        scraped_product = Product(product)
        product_element = WebDriverWait(driver, LONG_TIMEOUT).until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//div[@class='col-xs-8 col-sm-9 col-lg-8']/h2/a[text()='{product}']")))
        if product_element is not None:
            product_element.click()
            # Get the page source again to locate the unit with its subunits visible
            html_source = driver.page_source
            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(html_source, 'html.parser')

            # Now we are going to retrieve the P/N, EAN and product description
            p_n = soup.find('span', itemprop='sku')
            ean = soup.find('span', itemprop='gtin13')

            # Add the P/N and EAN to the product object if they were found
            if p_n is not None:
                scraped_product.add_p_n(p_n.text.strip())
            if ean is not None:
                scraped_product.add_ean(ean.text.strip())

            description_element = soup.select('div.col-md-8.inner-top-xs.inner-left-xs')[0]

            description = ''
            for tag in description_element.contents:
                if tag.name == 'p' or (isinstance(tag, NavigableString) and tag.strip() != ''):
                    description += tag.text.strip() + " "
                elif tag.name == 'ul' or tag.name == 'ol':
                    list_number = 1
                    for li in tag.contents:
                        if li.name == 'li':
                            description += str(list_number) + ") " + li.text.strip() + " "
                            list_number += 1

            scraped_product.add_description(description)
            scraped_products.append(scraped_product)
            # Check with BeautifulSoup if an element with title='Preguntas frecuentes' exists
            try:
                faq_element = driver.find_element(By.CLASS_NAME, 'icon-help')
                # Click on the FAQ button
                ActionChains(driver).move_to_element(faq_element).perform()
                faq_element.click()
                questions = [q.get_text(strip=True) for q in soup.select('div.row > div.col-sm-12.inner-top-xs > a.make-menos > h3.faq')]
                # Remove any questions that do not start with a number
                filtered_questions = [q for q in questions if re.match(r'^\d', q)]
                # Remove the number and the dot from the question
                clean_questions = [re.sub(r'^\d+\.\s*', '', q) for q in filtered_questions]

                answers = soup.select('div[class="m-b-lg"]')
                for idx, answer in enumerate(answers):
                    answer_description = ''
                    for answer_tag in answer.contents:
                        if answer_tag.name == 'p' or (isinstance(answer_tag, NavigableString) and answer_tag.strip() != ''):
                            answer_description += answer_tag.text.strip() + " "
                        elif answer_tag.name == 'ul' or answer_tag.name == 'ol':
                            list_number = 1
                            for li in answer_tag.contents:
                                if li.name == 'li':
                                    answer_description += str(list_number) + ") " + li.text.strip() + " "
                                    list_number += 1
                    # Create a Category object where the category name is 'Frequently Asked Questions', the Unit is the product name and the Subunit is the question and the Subunit description is the answer
                    faq = Category('FAQ', Unit(product, Subunit(clean_questions[idx], answer_description)))
                    scraped_faq.append(faq)

            except NoSuchElementException:
                continue
            except Exception as e:
                print(f"Error: {e}")
            finally:
                # Next, we will return to the previous page to continue scraping the next product
                driver.back()

    return scraped_products, scraped_faq


def scrap_myzone_tools(driver):
    tools_information = []
    tools_button = WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href='/herramientas']")))
    tools_button.click()

    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.presence_of_element_located((By.XPATH, "//h3[@class='sidelines text-center']")))

    # Get the page source again to locate the unit tools
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    tool_categories = soup.select('ul.metismenu > li')

    for category in tool_categories:
        category_name = category.select_one('a').text.strip()
        tools = [re.sub(r'^\d+\.\s*', '', t.text.strip()) for t in category.select('ul > li')]
        for t in tools:
            scraped_tool = Category('Tools', Unit(category_name, Subunit(t)))
            tools_information.append(scraped_tool)

    return tools_information

def scrap_airzone_compatibility(driver):
    scraped_airzone_compatibilities = []
    driver.get(MYZONE_COMPATIBILITY_URL)

    # Wait until any devices are loaded
    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.presence_of_element_located((By.XPATH, "//div[@class='caja el-col el-col-24 el-col-sm-14 "
                                                  "el-col-sm-offset-0 el-col-lg-7 el-col-lg-offset-3']")))
    # Get the page source again to locate the unit tools
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    collections = soup.find_all('div', class_='el-row')

    for collection in collections:
        collection_name = collection.find('h4', class_='collection-title').text.strip()
        collection_devices = [d.get_text(strip=True) for d in collection.select('div.caja.el-col.el-col-24.el-col-sm-14.el-col-sm-offset-0.el-col-lg-7.el-col-lg-offset-3 > div > div > p.title')]

        for device in collection_devices:
            scrap_device_compatibilities(driver, device)



    return scraped_airzone_compatibilities



def scrap_device_compatibilities(driver, device):
    scraped_device_compatibilities = []
    device_element = driver.find_element(By.XPATH, f"//p[@class='title' and text()='{device}']")
    ActionChains(driver).move_to_element(device_element).perform()
    device_element.click()

    # Wait until the device is loaded
    WebDriverWait(driver, LONG_TIMEOUT).until(
        EC.presence_of_element_located((By.XPATH, "//div[@class='caja el-col el-col-24 el-col-sm-14 "
                                                  "el-col-sm-offset-0 el-col-lg-7 el-col-lg-offset-3']")))
    # Get the page source again to locate the unit tools
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    articles = [a.get_text(strip=True) for a in soup.select('div.el-col.el-col-24 > p.title')]
    for article in articles:
        scraped_article_compatibilities = scrap_article_compatibilities(driver, article)

    return scraped_device_compatibilities

def scrap_article_compatibilities(driver, article):
    scraped_article_compatibilities = []
    article_element = driver.find_element(By.XPATH, f"//p[@class='title' and text()='{article}']")
    ActionChains(driver).move_to_element(article_element).perform()
    article_element.click()

    WebDriverWait(driver, LONG_TIMEOUT).until_not(
        EC.presence_of_element_located((By.XPATH, "//main[@class='el-loading-parent--relative']")))

    # Get the page source again to locate the article brands
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    article_brands = [b.get_text(strip=True) for b in soup.select('ul.el-scrollbar__view.el-select-dropdown__list > li > span')]

    for brand in article_brands:
        scraped_brand_compatibilities = scrap_brand_compatibilities(driver, brand)

    return scraped_device_compatibilities


def scrap_brand_compatibilities(driver, brand):
    scraped_brand_compatibilities = []
    brand_drop_down = driver.find_element(By.CLASS_NAME, 'el-input__inner')
    if brand_drop_down is not None:
        brand_drop_down.click()
        brand_option = driver.find_element(By.XPATH, f"//span[text()='{brand}']")
        brand_option.click()

    WebDriverWait(driver, LONG_TIMEOUT).until_not(
        EC.presence_of_element_located((By.XPATH, "//main[@class='el-loading-parent--relative']")))

    # Get the page source again to locate the brands models
    html_source = driver.page_source
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_source, 'html.parser')

    brand_models = [b.get_text(strip=True) for b in soup.select('ul.el-scrollbar__view.el-select-dropdown__list > li > span')]

    for model in brand_models:
        scraped_model_compatibilities = scrap_model_compatibilities(driver, model)

    return scraped_brand_compatibilities


def scrap_model_compatibilities(driver, model):
    scraped_model_compatibilities = []
    model_drop_down = driver.find_element(By.CLASS_NAME, 'el-input__inner')
    if model_drop_down is not None:
        model_drop_down.click()
        model_option = driver.find_element(By.XPATH, f"//span[text()='{model}']")
        model_option.click()

    WebDriverWait(driver, LONG_TIMEOUT).until_not(
        EC.presence_of_element_located((By.XPATH, "//p[@class='article']")))

    model_reference_element = driver.find_element(By.XPATH, "//p[@class='article']")
    model_reference_text = model_reference_element.text.strip()

    # The process for this model is finished and

    return scraped_brand_compatibilities
