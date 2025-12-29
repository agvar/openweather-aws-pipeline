from src.config_manager import get_config
from src.S3Operations import S3Operations
from src.DynamoDBOperations import DynamoDBOperations
from datetime import datetime
from src.logger import get_logger
import json

def update_progress_queue_status() -> None:
    config_params = get_config().config
    logger = get_logger(__name__)
    source_bucket = config_params.get("s3", {}).get("buckets", {}).get("source_bucket")
    source_prefix = config_params.get("s3", {}).get("buckets", {}).get("source_prefix")
    region = config_params.get("aws", {}).get("region", "us-east-1")
    s3Operations = S3Operations(source_bucket, region)
    dynamodb= DynamoDBOperations(region)
    control_table_queue: str = (
    config_params.get("dynamodb", {}).get("tables", {}).get("control_table_queue")
    )
    control_table_progress: str = (
    config_params.get("dynamodb", {}).get("tables", {}).get("control_table_progress")
    )
    s3_object_list = s3Operations.list_all_objects(
            source_prefix=source_prefix, extension='json'
    )
    logger.info(f"list of s3 objects: {s3_object_list}")
    exit(0)
    try:
        for object in s3_object_list:
            parts = object.split("/")
            zip_code = parts[7]
            year, month , day = parts[4], month = parts[5], day = parts[6]
            item_id = f"{zip_code}#US#{year}-{month}-{day}"
            result_query = dynamodb.update_item(
            control_table_queue,
            {"item_id": item_id},
            "SET #status= :completed, completed_at = :now",
            {":completed": "completed", ":now": datetime.now().isoformat()},
            {"#status": "status"},
            )
            result_progress = dynamodb.update_item(
            control_table_progress,
            {"job_id": "historical_collection"},
            "SET completed_items = completed_items + :inc , \
            daily_calls_used = daily_calls_used + :inc, \
            remaining_items = remaining_items - :inc ",
            {":inc": 1},
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
                            "country_code": "US",
                            "date": f'{year}-{month}-{day}',
                        }
                    ),
                }
            else:
                dynamodb.update_item(
                    control_table_queue,
                    {"item_id": item_id},
                    "SET #status = :failed, retry_count = retry_count + :inc, error_message= :error",
                    {":failed": "failed", ":inc": 1, ":error": "Error in updating item"},
                    {"#status": "status"},
                )
                logger.error(f"Error in updating item  of id: {item_id} in {control_table_queue}")
                raise ValueError(f"Error in updating item  of id: {item_id}")
    except Exception as e:
        dynamodb.update_item(
            control_table_queue,
            {"item_id": item_id},
            "SET #status = :failed, retry_count = retry_count + :inc, error_message= :error",
            {":failed": "failed", ":inc": 1, ":error": str(e)},
            {"#status": "status"},
        )
        logger.error(f"Error in lambda handler : {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


if __name__=="__main__":
    update_progress_queue_status()