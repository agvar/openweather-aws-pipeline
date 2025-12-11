from WeatherDataCollector import WeatherDataCollector
import json
from typing import Dict, Any
from logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Lambda function started")
    logger.info(f"Event:{json.dumps(event)}")
    try:
        logger.info("Instantiating WeatherDataCollector")
        weatherCollector = WeatherDataCollector()

        logger.info("Starting weather data collection")
        weatherCollector.collect_weather_data()

        logger.info("weather data collection completed successfully")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Weather data collection complete"}),
        }
    except Exception as e:
        logger.error(f"Error in lambda handler : {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": e, "request_id": context.request_id}),
        }
