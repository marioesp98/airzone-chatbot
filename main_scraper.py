import asyncio
import os
import time

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
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    deployment_option = os.environ.get('DEPLOYMENT_OPTION')

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
        logging.info("Setting up the connection to the database...")
        # Create a MongoDB client
        if deployment_option == 'ON_PREMISE':
            client = MongoClient(host=db_host, port=int(db_port), username=db_user, password=db_password)
        else:
            client = MongoClient(f'mongodb://{db_user}:{db_password}@{db_name}.cluster-ca1pg02lwckr.eu-west-3.docdb'
                             f'.amazonaws.com:{db_port}/?tls=true&tlsCAFile=src/global-bundle.pem&replicaSet=rs0'
                             f'&readPreference=secondaryPreferred&retryWrites=false')

        db = client[db_name]
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
        logging.error(f"Failed to connect to database: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    airzone_main_scraper()
