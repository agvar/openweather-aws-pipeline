from WeatherDataCollector import WeatherDataCollector
import json
from typing import Dict, Any
from logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Weather collector Lambda function ")
    try:
        logger.info("Reading WeatherCollector variables")
        logger.info("Instantiating WeatherDataCollector")
        weatherCollector = WeatherDataCollector()
        zipcode = event.get("zipcode")
        country_code = event.get("country_code")
        process_day = event.get("process_day")
        if zipcode and country_code and process_day:
            weatherCollector.collect_weather_data(zipcode, country_code, process_day)
        else:
            raise ValueError(
                f"Missing value for zipcode:{zipcode} "
                f"country code:{country_code} "
                f"or process_day:{process_day}"
            )

        logger.info("weather data collection completed successfully")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Weather data collection complete",
                    "zipcode": {zipcode},
                    "country_code": {country_code},
                    "process_day": {process_day},
                }
            ),
        }
    except Exception as e:
        logger.error(f"Error in lambda handler : {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": e}),
        }
