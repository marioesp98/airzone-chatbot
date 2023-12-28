import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup
from bs4 import NavigableString
import pandas as pd
import time

from resources.general_functions import insert_df_into_db, calculate_hash


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def process_subunit(session, category_name, unit_name, subunit):
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
    if re.match(r'^\d+\.', question.strip()):
        return 'Preguntas Frecuentes'
    else:
        return 'Autodiagnostico'


async def process_product(session, category_name, unit_name, subunit_name, product_name, product_endpoint):
    faqs = []
    product_response = await fetch(session, f"https://myzone.airzone.es{product_endpoint}")

    # Remove any <br> from the response before continuing
    filtered_response = product_response.replace('<br/>', '').replace('<br />', '').replace('</br>', '').replace('\n', '')
    product_soup = BeautifulSoup(filtered_response, 'html.parser')

    p_n = product_soup.find('span', itemprop='sku')
    ean = product_soup.find('span', itemprop='gtin13')

    # Add the P/N and EAN to the product if they were found
    product_p_n = ''
    if p_n is not None:
        product_p_n = p_n.text.strip()
    product_ean = ''
    if ean is not None:
        product_ean = ean.text.strip()

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

    # Include primary_hash_id and mod_hash_id
    primary_hash_id_data = f"{category_name}{unit_name}{subunit_name}{product_name}"
    primary_hash_id = calculate_hash(primary_hash_id_data)

    mod_hash_id_data = f"{clean_final_description}"
    mod_hash_id = calculate_hash(mod_hash_id_data)
    product_dict = {'primary_hash_id': primary_hash_id, 'mod_hash_id': mod_hash_id,
                    'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'), 'category': category_name,
                    'unit': unit_name, 'subunit': subunit_name, 'product': product_name,
                    'p_n': product_p_n, 'ean': product_ean, 'description': clean_final_description}

    # Check also if there are FAQs for this product
    questions = [q.get_text(strip=True) for q in
                 product_soup.select('div.row > div.col-sm-12.inner-top-xs > a.make-menos > h3.faq') if
                 q.get_text(strip=True) != '' and 'estado de los led' not in q.get_text(strip=True).lower()]

    # Create a list of tuples with each question and its type (the regex removes the number and dot at the beginning
    # of the question)
    question_tuples = [(re.sub(r'^\d+\.\s*', '', question), determine_question_type(question)) for question in
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

            # Include primary_hash_id and mod_hash_id
            faq_primary_hash_id_data = f"{question_tuples[idx][0]}"
            faq_primary_hash_id = calculate_hash(faq_primary_hash_id_data)

            faq_mod_hash_id_data = f"{clean_final_answer}"
            faq_mod_hash_id = calculate_hash(faq_mod_hash_id_data)

            faq = {'primary_hash_id': faq_primary_hash_id, 'mod_hash_id': faq_mod_hash_id,
                   'uploaded_date': time.strftime('%Y-%m-%d %H:%M:%S'), 'support_source': 'Product FAQs', 'category': 'Soporte Técnico',
                   'unit': question_tuples[idx][1], 'subunit': question_tuples[idx][0],
                   'type': '', 'description': clean_final_answer}
            faqs.append(faq)
        except IndexError:
            print(f"IndexError: {category_name}, {unit_name}, {subunit_name}, {product_name}")

    product_dict['faqs'] = faqs

    print(product_dict)

    return product_dict


async def airzone_products_scraper():
    async with aiohttp.ClientSession() as session:
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

        result = await asyncio.gather(*tasks)
        product_list = [product for sublist in result for product in sublist]
        # Extract all faqs elements from the product_list 'faqs' item in each product
        faqs = [faq for product in product_list for faq in product['faqs']]

        product_df = pd.DataFrame(product_list, columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'category',
                                                         'unit', 'subunit', 'product', 'p_n', 'ean', 'description'])

        product_query = "INSERT INTO product (primary_hash_id, mod_hash_id, uploaded_date, category, unit, subunit, " \
                        "product, " \
                        "p_n, ean, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        insert_df_into_db(product_df, product_query, 'product')

        faq_df = pd.DataFrame(faqs, columns=['primary_hash_id', 'mod_hash_id', 'uploaded_date', 'support_source', 'category',
                                                     'unit', 'subunit', 'type', 'description'])

        faq_query = "INSERT INTO support (primary_hash_id, mod_hash_id, uploaded_date, support_source, category, unit, subunit, type, " \
                    "description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        insert_df_into_db(faq_df, faq_query, 'support')
