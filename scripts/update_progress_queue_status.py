from openweather_pipeline.config_manager import get_config
from openweather_pipeline.s3_operations import S3Operations
from openweather_pipeline.dynamodb_operations import DynamoDBOperations
from datetime import datetime
from openweather_pipeline.logger import get_logger
import json
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

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
    logger.info(f"Count of s3 objects in list: {len(s3_object_list)}")
    try:
        for object in s3_object_list:
            parts = object.split("/")
            zip_code = parts[7]
            year_part, month_part , day_part = parts[4], parts[5], parts[6]
            year = year_part.split("=")[1]
            month = month_part.split("=")[1]
            day = day_part.split("=")[1]
            item_id = f"{zip_code}#US#{year}-{month}-{day}"

            try:
                result_query = dynamodb.update_item(
                table_nm=control_table_queue,
                key={"item_id": item_id},
                update_expression="SET #status= :completed, completed_at = :now",
                condition_expression=Attr('status').eq('pending') & Attr('item_id').exists(),
                expression_attrib_values={":completed": "completed", ":now": datetime.now().isoformat()},
                expression_attrib_names={"#status": "status"},
                )

                result_progress = dynamodb.update_item(
                table_nm=control_table_progress,
                key={"job_id": "historical_collection"},
                update_expression="SET completed_items = completed_items + :inc , \
                remaining_items = remaining_items - :inc ",
                expression_attrib_values={":inc": 1},
                )
                if result_query and result_progress:
                    logger.info(
                        f"Update complete for {item_id} in {control_table_progress} ,{control_table_queue}"
                    )
                else:
                    logger.error(f"Error in updating item  of id: {item_id} in {control_table_queue}")
                    raise ValueError(f"Error in updating item  of id: {item_id}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    logger.info(f"Skipping update of item_id :{item_id} as it is in a completed status.")
                    continue
                else:
                    raise
                
    except Exception as e:
        logger.error(f"Error : {str(e)}", exc_info=True)
        raise

if __name__=="__main__":
    update_progress_queue_status()