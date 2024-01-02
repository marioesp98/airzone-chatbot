import json

from src.main_scraper import airzone_main_scraper


def lambda_handler(event, context):
    try:
        # Run the main scraper
        airzone_main_scraper()
        return {
            'statusCode': 200,
            'body': json.dumps('Airzone scraper finished successfully')
        }
    except Exception as e:
        # If an error occurs, return an error message
        return {
            'statusCode': 500,
            'body': json.dumps({'Error: ': str(e)})
        }
