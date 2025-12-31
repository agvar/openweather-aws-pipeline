from typing import Dict, Any, List, Optional
from openweather_pipeline.logger import get_logger
from openweather_pipeline.config_manager import get_config
from openweather_pipeline.dynamodb_operations import DynamoDBOperations
from openweather_pipeline.models.collection_models import CollectionProgress, CollectionQueueItem
from datetime import datetime, timedelta

logger = get_logger(__name__)


def histGen_lambda_handler(event: Dict[Any, Any], context: Any) -> Optional[Dict[Any, Any]]:
    try:
        config_params = get_config().config
        control_table_queue: str = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_queue")
        )
        control_table_progress: str = (
            config_params.get("dynamodb", {}).get("tables", {}).get("control_table_progress")
        )
        daily_call_limit = config_params.get("app", {}).get("daily_call_limit", 950)
        zipcodes: List = config_params.get("app", {}).get("zipcodes", [])
        weather_start_dt: str = config_params.get("app", {}).get("weather_start_dt")
        weather_end_dt: str = config_params.get("app", {}).get("weather_end_dt")

        start_dt = datetime.strptime(weather_start_dt, "%Y-%m-%d").date()
        end_dt = datetime.strptime(weather_end_dt, "%Y-%m-%d").date()

        region: str = config_params.get("aws", {}).get("region")
        dynamodb = DynamoDBOperations(region)

        check_table_isEmpty = dynamodb.check_table_isEmpty(control_table_queue)
        if check_table_isEmpty:
            logger.info(f"Table {control_table_queue} is empty")
            queue_items = []
            current = start_dt
            while current <= end_dt:
                for zipcode in zipcodes:
                    zip = zipcode.get("zip_code")
                    country_code = zipcode.get("country_code")
                    current_str = current.strftime("%Y-%m-%d")
                    queue_items.append(
                        CollectionQueueItem(
                            item_id=f"{zip}#{country_code}#{current_str}",
                            zip_code=zip,
                            country_code=country_code,
                            date=current_str,
                            status="pending",
                            retry_count=0,
                        )
                    )
                current += timedelta(days=1)
            logger.info(f"Created  {len(queue_items)} items fot insert into {control_table_queue}")
            dynamodb.batch_put_items(queue_items, control_table_queue)
            logger.info(f"Inserted {len(queue_items)} items in {control_table_queue}")

            intial_progress_record = CollectionProgress(
                job_id="historical_collection",
                zipcodes=zipcodes,
                total_items=len(queue_items),
                completed_items=0,
                remaining_items=len(queue_items),
                daily_calls_limit=daily_call_limit,
                daily_calls_used=0,
                status="in_progress",
            )
            dynamodb.put_item(intial_progress_record, control_table_progress)
            logger.info(f"Inserted initial record into {control_table_progress}")

        # current items to process

        progress = dynamodb.get_item(
            CollectionProgress, control_table_progress, {"job_id": "historical_collection"}
        )
        if not progress:
            logger.error(f"Unable to find {control_table_progress} record for job_id")
            raise ValueError(f"Unable to find {control_table_progress} record for job_id")
        today = datetime.now().date()
        last_run_val = progress.last_run
        if last_run_val is None:
            last_run_dt = datetime(1973, 1, 1).date()
        else:
            last_run_dt = datetime.strptime(last_run_val, "%Y-%m-%d").date()

        if today >= last_run_dt:
            progress.daily_calls_used = 0

        if progress.daily_calls_used >= progress.daily_calls_limit:
            logger.info(f"Daily limit of {progress.daily_calls_limit} calls reached")
            return {
                "statusCode": 200,
                "items": [],
                "count": 0,
            }
        calls_remaining = progress.daily_calls_limit - progress.daily_calls_used

        pending_items: Optional[List[CollectionQueueItem]] = dynamodb.query_table_all_fields(
            CollectionQueueItem,
            control_table_queue,
            "status-date-index",
            "#status = :pending",
            {"#status": "status"},
            {":pending": "pending"},
            limit_rows=calls_remaining,
        )
        if pending_items:
            logger.info(f"Read {len(pending_items)} from {control_table_queue} ")
            return {
                "statusCode": 200,
                "items": [item.model_dump() for item in pending_items],
                "count": len(pending_items),
            }
        else:
            logger.error(f"Error retrieving items from {control_table_queue}", exc_info=True)
            raise ValueError(f"Error retrieving items from {control_table_queue}")
    except Exception as e:
        logger.error(f"Error in lambda handler{__name__}, {e}", exc_info=True)
        raise


if __name__ == "__main__":
    histGen_lambda_handler(event={"key": 1, "value": 1}, context=None)
