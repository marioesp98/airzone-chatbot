import asyncio
import os
import mysql.connector
import requests
import logging
import logging.config
import yaml

from src.utils.general_functions import setup_logging
from src.websites.airzonecontrol.airzonecontrol_functions import academia_scraper, partner_scraper
from src.websites.airzonecontrol.general_information_scraper import general_information_scraper
from src.websites.myzone.myzone_products_scraper import airzone_products_scraper
from src.websites.support.support_functions import support_scraper


def airzone_main_scraper():
    global db_connection
    api_key = os.environ.get('AIRZONE_API_KEY')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_database = os.environ.get('DB_DATABASE')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'apikey': api_key,
        'app-locale': 'es',
        'app-market': 'ib'
    }

    # Check if the environment variables are set
    if not api_key:
        raise ValueError("AIRZONE_API_KEY not set in the environment variables.")

    try:
        # Set up the logging configuration based on the deployment option (On-premise or Cloud)
        setup_logging()

        # Create a MySQL connection
        db_connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_database
        )
        session = requests.Session()
        session.headers.update(headers)

        logging.info("Starting the Airzone scraper...")
        # Run all the scrapers
        academia_scraper(session, db_connection)
        general_information_scraper(session, db_connection)
        partner_scraper(session, db_connection)
        support_scraper(session, db_connection)
        asyncio.run(airzone_products_scraper(db_connection))

    except mysql.connector.Error as e:
        print(f"Failed to connect to database: {e}")

    except Exception as e:
        logging.exception("Exception occurred")

    return "Function executed successfully"


if __name__ == "__main__":
    airzone_main_scraper()
