from typing import Dict, Any, List
from logger import get_logger
from config_manager import get_config
from DynamoDBOperations import DynamoDBOperations
from models.collection_models import CollectionProgress, CollectionQueueItem

logger = get_logger(__name__)


def histGen_lambda_handler(event: Dict[Any, Any], context: Any) -> List[Dict[Any, Any]]:
    try:
        config_params = get_config().config
        weather_historical_flag: bool = config_params.get("app", {}).get("weather_historical_flag")
        control_table_queue: bool = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_queue")
        )
        control_table_progress: bool = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_progress")
        )

        region: str = config_params.get("aws", {}).get("region")
        dynamodb = DynamoDBOperations(region)

        progress = dynamodb.get_item(control_table_progress, {"job_id": "historical_collection"})
        if "Item" not in progress:
            intial_progress_record = {
                "job_id": "historical_collection",
                "total_items": 0,
                "completed_items": 0,
                "remaining_items": 0,
                "daily_calls_limit": 950,
                "daily_calls_used": 0,
                "status": "in_progress",
            }
            dynamodb.put_item(control_table_progress, intial_progress_record)
            logger.info(f"Inserted intial record into {control_table_progress}")

        logger.info(f"Value of key: {progress.get('Item')}")
        return []
    except Exception as e:
        logger.error(f"Error in lambda handler{__name__}", exc_info=True)



if __name__ == "__main__":
    histGen_lambda_handler(event={1, 1}, context=None)
