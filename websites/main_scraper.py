import asyncio
import os
import requests

from websites.airzonecontrol.airzonecontrol_functions import academia_scraper, partner_scraper
from websites.airzonecontrol.general_information_scraper import general_information_scraper
from websites.myzone.myzone_products_scraper import airzone_products_scraper
from websites.support.support_functions import support_scraper

api_key = os.environ.get('AIRZONE_API_KEY')

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'apikey': api_key,
    'app-locale': 'es',
    'app-market': 'ib'
}
session = requests.Session()
session.headers.update(headers)

# Run all the scrapers
support_scraper(session)
academia_scraper(session)
partner_scraper(session)
general_information_scraper(session)
asyncio.run(airzone_products_scraper())
