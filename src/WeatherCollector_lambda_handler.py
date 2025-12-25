from WeatherDataCollector import WeatherDataCollector
import json
from typing import Dict, Any
from logger import get_logger
from DynamoDBOperations import DynamoDBOperations
from config_manager import get_config
from datetime import datetime

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Weather collector Lambda function ")
    try:
        logger.info("Instantiating WeatherDataCollector")
        config_params = get_config().config
        region: str = config_params.get("aws", {}).get("region")
        dynamodb = DynamoDBOperations(region)
        weatherCollector = WeatherDataCollector()
        control_table_queue: str = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_queue")
        )
        zipcode = event.get("zipcode")
        country_code = event.get("country_code")
        process_day = event.get("process_day")
        item_id = event.get("item_id")
        if zipcode and country_code and process_day:
            weatherCollector.collect_weather_data(zipcode, country_code, process_day)
        else:
            raise ValueError(
                f"Missing value for zipcode:{zipcode} "
                f"country code:{country_code} "
                f"or process_day:{process_day}"
            )

        logger.info("weather data collection completed successfully")
        result = dynamodb.update_item(
            control_table_queue,
            {"item_id": item_id},
            "SET status= :completed, completed_at = :now",
            {":completed": "completed", ":now": datetime.now().isoformat()},
        )
        if result:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Weather data collection complete",
                        "zipcode": zipcode,
                        "country_code": country_code,
                        "process_day": process_day,
                    }
                ),
            }
        else:
            result = dynamodb.update_item(
                control_table_queue,
                {"item_id": item_id},
                "SET status= :failed, retry_count = retry_count + :inc, error_message= :error",
                {":failed": "failed", ":inc": 1, ":error": "Error in updating item"},
            )
            logger.error(f"Error in updating item  of id: {item_id} in {control_table_queue}")
            raise ValueError(f"Error in updating item  of id: {item_id}")
    except Exception as e:
        result = dynamodb.update_item(
            control_table_queue,
            {"item_id": item_id},
            "SET status= :failed, retry_count = retry_count + :inc, error_message= :error",
            {":failed": "failed", ":inc": 1, ":error": str(e)},
        )
        logger.error(f"Error in lambda handler : {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": e}),
        }
