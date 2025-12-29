from logger import get_logger
from config_manager import get_config
from typing import Dict, Type, TypeVar, Optional, List
import boto3
from pydantic import BaseModel, ValidationError

logger = get_logger(__name__)
T = TypeVar("T", bound=BaseModel)


class DynamoDBOperations:

    def __init__(self, region: str):
        try:
            logger.info("Starting dynamodb initialization")
            self.config = get_config().config
            self.dynamoDb = boto3.resource("dynamodb", region_name=region)
        except Exception as e:
            logger.erorr(f" Exception when initializing DynamoDB {e}", exc_info=True)

    def get_item(self, model_class: Type[T], table_nm: str, key: Dict[str, str]) -> Optional[T]:
        try:
            table = self.dynamoDb.Table(table_nm)
            response = table.get_item(Key=key)
            if "Item" not in response:
                return None
            item = model_class(**response["Item"])
            return item
        except Exception as e:
            logger.error(f"Error getting item {key} from {table},{e}", exc_info=True)
            raise

    def query_table_all_fields(
        self,
        model_class: Type[T],
        table_nm: str,
        index_name: str,
        key_condition_expression: str,
        expression_attrib_names: Dict[str, str],
        expression_attrib_values: Dict[str, str],
        limit_rows: int,
        **kwargs: Optional[Dict[str, str]],
    ) -> Optional[List[T]]:
        try:
            table = self.dynamoDb.Table(table_nm)
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeNames=expression_attrib_names,
                ExpressionAttributeValues=expression_attrib_values,
                Limit=limit_rows,
                **kwargs,
            )
            items = []
            if "Items" not in response:
                return None
            for item in response.get("Items", []):
                try:
                    items.append(model_class(**item))
                except ValidationError as e:
                    logger.warning(f"Skipping invalid item,{e}")
                    continue
            logger.info(f"Retrieved {len(items)} items from {table}")
            return items
        except Exception as e:
            logger.error(
                f"Error getting item {key_condition_expression} from dynamodb {table},{e}",
                exc_info=True,
            )
            raise

    def put_item(self, model_instance: T, table_nm: str) -> bool:
        try:
            item_dict = model_instance.dict()
            table = self.dynamoDb.Table(table_nm)
            table.put_item(Item=item_dict)
            return True
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise

    def batch_put_items(self, items: List[T], table_nm: str) -> bool:
        try:

            table = self.dynamoDb.Table(table_nm)
            with table.batch_writer() as batch:
                for item in items:
                    item_dict = item.dict()
                    batch.put_item(Item=item_dict)
                    logger.info(f"Inserted batch of items into {table_nm}")
                return True
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise

    def check_table_isEmpty(self, table_nm: str) -> bool:
        try:
            table = self.dynamoDb.Table(table_nm)
            first_response = table.scan(Limit=1)
            item = first_response.get("Items", [])
            if item:
                return False
            else:
                return True
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise

    def update_item(
        self,
        table_nm: str,
        key: Dict[str, str],
        update_expression: str,
        expression_attrib_values: Dict[str, str],
        expression_attrib_names: Optional[Dict[str, str]] = None,
    ) -> bool:
        try:
            table = self.dynamoDb.Table(table_nm)
            update_params: Dict = {
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeValues": expression_attrib_values,
            }

            if expression_attrib_names:
                update_params["ExpressionAttributeNames"] = expression_attrib_names
            table.update_item(**update_params)
            return True
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table_nm},{e}", exc_info=True)
            raise


if __name__ == "__main__":
    config = get_config().config
    region = config.get("aws", {}).get("region", "us-east-1")
    dynamodb = DynamoDBOperations(region)
