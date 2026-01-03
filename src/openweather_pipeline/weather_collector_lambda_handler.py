from openweather_pipeline.weather_data_collector import WeatherDataCollector
import json
from typing import Dict, Any
from openweather_pipeline.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Weather collector Lambda function ")
    try:
        logger.info("Instantiating WeatherDataCollector")
        weatherCollector = WeatherDataCollector()

        zip_code = event.get("zip_code")
        country_code = event.get("country_code")
        date = event.get("date")
        item_id = event.get("item_id")

        if zip_code and country_code and date and item_id:
            weatherCollector.collect_weather_data(zip_code, country_code, date, item_id)
        else:
            raise ValueError(
                f"Missing value for zipcode:{zip_code} "
                f"country code:{country_code} "
                f"or process_day:{date}"
                f"or item_id:{item_id}"
            )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Weather data collection complete",
                    "zip_code": zip_code,
                    "country_code": country_code,
                    "date": date,
                }
            ),
        }
    except Exception as e:
        logger.error(f"Error in lambda handler : {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


if __name__ == "__main__":
    event = {
        "item_id": "10001#US#2020-01-02",
        "zip_code": "10001",
        "country_code": "US",
        "date": "2020-01-02",
        "status": "pending",
        "retry_count": "0",
        "last_attempt": None,
        "completed_at": None,
        "error_message": None,
    }
    lambda_handler(event, None)
