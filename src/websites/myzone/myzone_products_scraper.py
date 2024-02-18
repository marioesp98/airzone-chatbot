import asyncio
import logging
import re
import aiohttp
from bs4 import BeautifulSoup
from bs4 import NavigableString
import pandas as pd
import time

from src.utils.general_functions import calculate_hash, insert_df_into_db, fetch, split_text_into_chunks


async def process_subunit(session, category_name, unit_name, subunit):
    """
    Process a subunit.
    :param session: aiohttp session
    :param category_name: category name
    :param unit_name: unit name
    :param subunit: subunit to process
    :return:
    """
    subunit_name = subunit.text.split('(')[0].strip()
    subunit_endpoint = subunit.select('a')[0]['href']
    subunit_response = await fetch(session, f"https://myzone.airzone.es{subunit_endpoint}")
    subunit_soup = BeautifulSoup(subunit_response, 'html.parser')

    products = subunit_soup.select('div.col-xs-4.col-sm-3.col-lg-4.inner-bottom-xs.text-center.li-img-holder')

    tasks = []
    for product in products:
        product_endpoint = product.select('a')[0]['href']
        product_name = product.find_next_sibling('div').select('h2')[0].text

        tasks.append(process_product(session, category_name, unit_name, subunit_name, product_name, product_endpoint))

        # The previous line does not take into account that process_product returns two values, so we need to change
        # it to: product_dict, faqs = await process_product(session, category_name, unit_name, subunit_name,
        # product_name, product_endpoint) tasks.append((product_dict, faqs))
    return await asyncio.gather(*tasks)


# If question starts with a number followed by a dot, it will be a 'Preguntas Frecuentes' question
def determine_question_type(question):
    """
    Determine the type of question ('Preguntas Frecuentes' or 'Autodiagnostico').
    :param question:  question to determine its type
    :return: question type
    """
    if re.match(r'^\d+\.', question.strip()):
        return 'Preguntas Frecuentes'
    else:
        return 'Autodiagnostico'


async def process_product(session, category_name, unit_name, subunit_name, product_name, product_endpoint):
    """
    Process a product, including its FAQs.
    :param session: aiohttp session
    :param category_name: category name
    :param unit_name: unit name
    :param subunit_name: subunit name
    :param product_name: product name
    :param product_endpoint: product endpoint
    :return: product dictionary with faqs list
    """
    product_chunks_list = []
    faqs = []
    product_response = await fetch(session, f"https://myzone.airzone.es{product_endpoint}")

    # Remove any <br> from the response before continuing
    filtered_response = product_response.replace('<br/>', '').replace('<br />', '').replace('</br>', '').replace('\n',
                                                                                                                 '')
    product_soup = BeautifulSoup(filtered_response, 'html.parser')

    raw_description = product_soup.find('div', itemprop='description')
    final_description = ''
    for tag in raw_description.contents:
        if tag.name == 'p' or (isinstance(tag, NavigableString) and tag.strip() != ''):
            final_description += tag.text.strip() + " "
        elif tag.name == 'ul' or tag.name == 'ol':
            list_number = 1
            for li in tag.contents:
                if li.name == 'li':
                    final_description += str(list_number) + ") " + li.text.strip() + " "
                    list_number += 1

    # Clean the final description removing double whitespaces and whitespaces before a dot
    clean_final_description = final_description.replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                               " ").replace(
        "\n", " ").strip()
    chunks = split_text_into_chunks(clean_final_description, chunk_size=1000, chunk_overlap=200)

    for i, text in enumerate(chunks):
        # Calculate the hash_id based on the title and the description
        hash_id_data = f"{product_name}{text}"
        hash_id = calculate_hash(hash_id_data)

        chunk_dict = {
            'hash_id': hash_id,
            'upload_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source': 'Airzone Products',
            'title': product_name,
            'description': text
        }
        product_chunks_list.append(chunk_dict)

    # Check also if there are FAQs for this product
    questions = [q.get_text(strip=True) for q in
                 product_soup.select('div.row > div.col-sm-12.inner-top-xs > a.make-menos > h3.faq') if
                 q.get_text(strip=True) != '' and 'estado de los led' not in q.get_text(strip=True).lower()]

    # Create a list of tuples with each question and its type (the regex removes the number and dot at the beginning
    # of the question)
    clean_questions = [re.sub(r'^\d+\.\s*', '', question) for question in
                       questions]

    faq_answers = product_soup.select('div[class="m-b-lg"]')

    autodiagnosis_answers = product_soup.find('div', class_='faq_detalle m-b-lg')

    if autodiagnosis_answers:
        autodiagnosis_answers = autodiagnosis_answers.select('div.row > div.col-md-8')

        # Remove any autodiagnosis_answers where the next child element has class 'table table-bordered'
        for idx, answer in enumerate(autodiagnosis_answers):
            if answer.find('table'):
                autodiagnosis_answers.pop(idx)
        answers = faq_answers + autodiagnosis_answers
    else:
        answers = faq_answers

    for idx, answer in enumerate(answers):
        try:
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

            # Clean the final answer removing double whitespaces and whitespaces before a dot
            clean_final_answer = answer_description.replace(" .", ".").replace("   ", " ").replace("  ",
                                                                                                   " ").replace(
                "\n", " ").strip()

            chunks = split_text_into_chunks(clean_final_answer, chunk_size=1000, chunk_overlap=200)

            for i, text in enumerate(chunks):
                # Calculate the hash_id based on the title and the description
                hash_id_data = f"{clean_questions[idx]}{text}"
                hash_id = calculate_hash(hash_id_data)

                faq_chunk = {
                    'hash_id': hash_id,
                    'upload_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'source': 'Product FAQs',
                    'title': clean_questions[idx],
                    'description': text
                }
                faqs.append(faq_chunk)

        except IndexError:
            print(f"IndexError: {category_name}, {unit_name}, {subunit_name}, {product_name}")

    product_dict = {'products': product_chunks_list, 'faqs': faqs}

    return product_dict


async def airzone_products_scraper(db):
    """
    Scrape the products from the Myzone website and their FAQs.
    :param db_connection: MySQL's connection object
    """
    try:
        async with aiohttp.ClientSession() as session:
            logging.info("Starting the 'Myzone Products' scraper...")
            products_endpoint = 'https://myzone.airzone.es/productos/'
            response = await fetch(session, products_endpoint)
            soup = BeautifulSoup(response, 'html.parser')

            categories = soup.find_all('h3', {'class': 'sidelines text-center'})

            tasks = []
            for category in categories:
                category_name = category.text.strip()
                units = category.find_next_sibling('ul').select('li.categoria')

                for unit in units:
                    unit_name = unit.select('a > span.sidebar-nav-item')[0].text
                    subunits = unit.select('ul > li')

                    for subunit in subunits:
                        # If subunit text does not contain 'Ver todos'
                        if 'Ver todos' not in subunit.text:
                            tasks.append(process_subunit(session, category_name, unit_name, subunit))

            # Wait for all tasks to finish
            result = await asyncio.gather(*tasks)

            product_list = [product for sublist in result for chunk_list in sublist for product in
                            chunk_list['products']]
            # Extract all faqs elements from the product_list 'faqs' item in each product
            faqs = [faq for sublist in result for chunk_list in sublist for faq in chunk_list['faqs']]

            product_df = pd.DataFrame(product_list)
            product_collection = db['product']
            insert_df_into_db(product_collection, product_df)

            faq_df = pd.DataFrame(faqs)
            support_collection = db['support']
            insert_df_into_db(support_collection, faq_df)

    except aiohttp.ClientError as e:
        logging.error(f"Aiohttp client error: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred in 'airzone_products_scraper': {str(e)}")
