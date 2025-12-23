from logger import get_logger
from config_manager import get_config
from typing import Dict, Any
import boto3

logger = get_logger(__name__)


class DynamoDBOperations:
    def __init__(self, region: str):
        try:
            logger.info("Starting dynamodb initialization")
            config = get_config().config

            self.dynamoDb = boto3.resource("dynamodb", region_name=region)
        except Exception as e:
            logger.erorr(f" Exception when initializing DynamoDB {e}", exc_info=True)

    def get_item(self, table_nm: str, key: Dict[str, str]):
        try:
            table = self.dynamoDb.Table(table_nm)
            item = table.get_item(Key=key)
            return item
        except Exception as e:
            logger.error(f"Error getting item {key} from {table},{e}", exc_info=True)

    def query_table(
        self,
        table: str,
        key_condition_expression: str,
        expression_attrib_names: str,
        expression_attrib_values: str,
    ):
        pass

    def put_item(self, table: str, item: Dict[Any, Any]):
        pass


if __name__ == "__main__":
    config = get_config().config
    region = config.get("aws", {}).get("region", "us-east-1")
    dynamodb = DynamoDBOperations(region)
