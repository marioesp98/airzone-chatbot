import asyncio
import os
import requests
import logging
import logging.config
from pymongo import MongoClient, errors as pymongo_errors
from src.utils.general_functions import setup_logging
from src.websites.airzonecontrol.airzonecontrol_functions import academia_scraper, partner_scraper
from src.websites.airzonecontrol.general_information_scraper import general_information_scraper
from src.websites.myzone.myzone_products_scraper import airzone_products_scraper
from src.websites.support.support_functions import support_scraper


def airzone_main_scraper():
    global db_connection, client
    api_key = os.environ.get('AIRZONE_API_KEY')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('db_name')
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

        # Create a MongoDB client
        client = MongoClient(host=db_host, port=int(db_port), username=db_user, password=db_password)
        db = client['local']

        session = requests.Session()
        session.headers.update(headers)

        logging.info("Starting the Airzone scraper...")
        # Run all the scrapers
        support_scraper(session, db)
        academia_scraper(session, db)
        general_information_scraper(session, db)
        partner_scraper(session, db)
        asyncio.run(airzone_products_scraper(db))
        logging.info("Airzone scraper finished successfully")

    except pymongo_errors.PyMongoError as e:
        print(f"Failed to connect to database: {e}")

    except Exception as e:
        logging.exception("Exception occurred")
    finally:
        client.close()


if __name__ == "__main__":
    airzone_main_scraper()
