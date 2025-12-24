from typing import Dict, Any, List
from logger import get_logger
from config_manager import get_config
from DynamoDBOperations import DynamoDBOperations
from models.collection_models import CollectionProgress, CollectionQueueItem
from datetime import datetime, timedelta

logger = get_logger(__name__)


def histGen_lambda_handler(event: Dict[Any, Any], context: Any) -> List[Dict[Any, Any]]:
    try:
        config_params = get_config().config
        weather_historical_flag: bool = config_params.get("app", {}).get("weather_historical_flag")
        control_table_queue: str = config_params.get("dynamodb", {}).get("tables", {}).get("control_table_queue")
        control_table_progress: str = config_params.get("dynamodb", {}).get("tables", {}).get("control_table_progress")
        zipcodes: List = config_params.get("app", {}).get("zipcodes", {})
        weather_year_start: int = int(config_params.get("app", {}).get("weather_year_start", {}))
        weather_year_end: List = int(config_params.get("app", {}).get("weather_year_end", {}))

        year_start_dt = datetime(weather_year_start,1,1)
        year_end_dt = datetime(weather_year_end,1,1)

        region: str = config_params.get("aws", {}).get("region")
        dynamodb = DynamoDBOperations(region)
        
        check_table_isEmpty = dynamodb.check_table_isEmpty(control_table_queue)
        if check_table_isEmpty:
            logger.info(f"Table {control_table_queue} is empty")
            queue_items = []
            current = year_start_dt
            while current <= year_end_dt:
                for zipcode in zipcodes:
                    zip = zipcode.get('zipcode')
                    country_code = zipcode.get('country_code')
                    current_str = current.strftime("%Y-%m-%d")
                    queue_items.append(
                        CollectionQueueItem(
                                item_id = f"{zip}#{country_code}#{current_str}",
                                zipcode = zip,
                                country_code = country_code,
                                date = current_str,
                                status = "pending",
                                retry_count = 0
                        )
                    )
                current += timedelta(days=1)
            logger.info(f"Created  {len(queue_items)} items fot insert into {control_table_queue}")
            dynamodb.batch_put_items(queue_items,control_table_queue)
            logger.info(f"Inserted {len(queue_items)} items in {control_table_queue}")

            intial_progress_record = CollectionProgress(
                job_id= "historical_collection",
                total_items= len(queue_items),
                completed_items= 0,
                remaining_items= len(queue_items),
                daily_calls_limit= 950,
                daily_calls_used= 0,
                status= "in_progress"
            )
            dynamodb.put_item(intial_progress_record, control_table_progress)
            logger.info(f"Inserted initial record into {control_table_progress}")
        
        #current items to process

        progress = dynamodb.get_item(
            CollectionProgress,
            control_table_progress,
            {"job_id": "historical_collection"}
        )

        today = datetime.now().date()
        last_run = progress.last_run
        if last_run is None:
            last_run = datetime(1973,1,1)
        else:
            last_run = last_run.strptime('%Y-%m-%d')

        if today >= last_run:
            progress.daily_calls_used = 0
        
        if progress.daily_calls_used >= progress.daily_calls_limit:
            logger.info(f"Daily limit of {progress.daily_calls_limit} calls reached")
            return
        calls_remaining = progress.daily_calls_limit - progress.daily_calls_used

        pending_items = dynamodb.query_table(
            CollectionQueueItem,
            control_table_queue,
            "status: pending",
            {":pending":"pending"},
            Limit= calls_remaining
        )
        logger.info(f"Read {len(pending_items)} from {control_table_queue} ")
        return pending_items
    except Exception as e:
        logger.error(f"Error in lambda handler{__name__}", exc_info=True)

if __name__ == "__main__":
    histGen_lambda_handler(event={1, 1}, context=None)
