from openweather_pipeline.weather_data_collector import WeatherDataCollector
import json
from typing import Dict, Any
from openweather_pipeline.logger import get_logger
from openweather_pipeline.dynamodb_operations import DynamoDBOperations
from openweather_pipeline.config_manager import get_config
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
        control_table_progress: str = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_progress")
        )
        zip_code = event.get("zip_code")
        country_code = event.get("country_code")
        date = event.get("date")
        item_id = event.get("item_id")
        if zip_code and country_code and date:
            weatherCollector.collect_weather_data(zip_code, country_code, date)
        else:
            raise ValueError(
                f"Missing value for zipcode:{zip_code} "
                f"country code:{country_code} "
                f"or process_day:{date}"
            )

        result_query = dynamodb.update_item(
            table_nm=control_table_queue,
            key={"item_id": item_id},
            update_expression="SET #status= :completed, completed_at = :now",
            expression_attrib_values={
                ":completed": "completed",
                ":now": datetime.now().isoformat(),
            },
            expression_attrib_names={"#status": "status"},
        )
        result_progress = dynamodb.update_item(
            table_nm=control_table_progress,
            key={"job_id": "historical_collection"},
            update_expression="SET completed_items = completed_items + :inc , \
            daily_calls_used = daily_calls_used + :inc, \
            remaining_items = remaining_items - :inc ",
            expression_attrib_values={":inc": 1},
        )
        if result_query and result_progress:
            logger.info(
                f"Update complete for {item_id} in {control_table_progress} ,{control_table_queue}"
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
        else:
            dynamodb.update_item(
                table_nm=control_table_queue,
                key={"item_id": item_id},
                update_expression="SET #status = :failed, retry_count = retry_count + :inc, \
                error_message= :error",
                expression_attrib_values={
                    ":failed": "failed",
                    ":inc": 1,
                    ":error": "Error in updating item",
                },
                expression_attrib_names={"#status": "status"},
            )
            logger.error(f"Error in updating item  of id: {item_id} in {control_table_queue}")
            raise ValueError(f"Error in updating item  of id: {item_id}")
    except Exception as e:

        dynamodb.update_item(
            table_nm=control_table_queue,
            key={"item_id": item_id},
            update_expression="SET #status = :failed, retry_count = retry_count + :inc, \
            error_message= :error",
            expression_attrib_values={":failed": "failed", ":inc": 1, ":error": str(e)},
            expression_attrib_names={"#status": "status"},
        )
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
