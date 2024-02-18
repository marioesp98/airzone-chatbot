import json
import sys
import traceback
from main_scraper import airzone_main_scraper


def lambda_handler(event, context):
    try:
        # Run the main scraper
        airzone_main_scraper()
        return {
            'statusCode': 200,
            'body': json.dumps('Airzone scraper finished successfully')
        }
    except Exception as e:
        # Log or handle the error
        print("An error occurred:", e)

        # Get error details
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_details = {
            "type": str(exc_type),
            "message": str(exc_value),
            "traceback": traceback.format_tb(exc_traceback)
        }
        print("Error details:", error_details)

        # Return an error response if needed
        return {
            "statusCode": 500,
            "body": "An error occurred"
        }
