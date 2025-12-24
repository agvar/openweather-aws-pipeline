from logger import get_logger
from config_manager import get_config
from models.collection_models import CollectionProgress,CollectionQueueItem
from typing import Dict, Any,Type, TypeVar, Optional,List
import boto3
from pydantic import BaseModel, ValidationError

logger = get_logger(__name__)
T = TypeVar('T', bound=BaseModel)

class DynamoDBOperations:

    def __init__(self, region: str):
        try:
            logger.info("Starting dynamodb initialization")
            self.config = get_config().config
            self.dynamoDb = boto3.resource("dynamodb", region_name=region)
        except Exception as e:
            logger.erorr(f" Exception when initializing DynamoDB {e}", exc_info=True)

    def get_item(
        self,
        model_class: Type[T] , 
        table_nm: str, 
        key: Dict[str, str]
    ) -> Optional[T]:
        try:
            table = self.dynamoDb.Table(table_nm)
            response = table.get_item(Key=key)
            if 'Item' not in response:
                return None
            item = model_class(**response['Item'])
            return item
        except Exception as e:
            logger.error(f"Error getting item {key} from {table},{e}", exc_info=True)
            raise

    def query_table(
        self,
        model_class: Type[T],
        table_nm: str,
        key_condition_expression: str,
        expression_attrib_values: Dict[str,str],
        limit_rows: int,
        **kwargs
    ) -> Optional[List[T]]:
        try:
            table = self.dynamoDb.Table(table_nm)
            response = table.query(
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attrib_values,
                Limit=limit_rows
                **kwargs
            )
            items=[]
            if 'Items' not in response:
                return None
            for item in response.get('Items',[]):
                try:
                    items.append(model_class(**item))
                except ValidationError as e:
                    logger.warning(f"Skipping invalid item")
                    continue
            logger.info(f"Retrieved {len(items)} items from {table}")
            return items
        except Exception as e:
            logger.error(f"Error getting item {key_condition_expression} from dynamodb {table},{e}", exc_info=True)
            raise


    def put_item(
        self, 
        model_class: Type[T],
        table_nm: str):
        try:
            item_dict = model_class.dict()
            table = self.dynamoDb.Table(table_nm)
            table.put_item(Item= item_dict)
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise

    def batch_put_items(
        self, 
        items: List[Type[T]],
        table_nm: str):
        try:
            
            table = self.dynamoDb.Table(table_nm)
            with table.batch_writer() as batch:
                for item in items:
                    item_dict = item.dict()
                    table.put_item(Item= item_dict)
                    logger.info(f"Inserted batch of items into {table_nm}")
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise

    def check_table_isEmpty(
        self,
        table_nm: str
    ) -> bin :
        try:
            table = self.dynamoDb.Table(table_nm)
            first_response = table.scan(Limit=1)
            item = first_response.get('Items',[])
            if item:
                return False
            else :
                return True
        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise
        
    def update_item(
        self,
        table_nm: str,
        key: Dict[str, str],
        update_expression : str,
        expression_attrib_values : Dict[str,str]
    )-> bin:
        try:
            self.dynamoDb.update_item(
                TableName=table_nm,
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attrib_values
            )

        except Exception as e:
            logger.error(f"Failed to write record from dynamodb {table},{e}", exc_info=True)
            raise



if __name__ == "__main__":
    config = get_config().config
    region = config.get("aws", {}).get("region", "us-east-1")
    dynamodb = DynamoDBOperations(region)
