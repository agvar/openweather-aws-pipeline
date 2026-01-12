from openweather_pipeline.s3_operations import S3Operations;
from openweather_pipeline.config_manager import get_config
from openweather_pipeline.logger import get_logger
from openweather_pipeline.dynamodb_operations import DynamoDBOperations

def repartition_s3_objects() -> None :
    config_params = get_config().config
    logger = get_logger(__name__)
    source_bucket = "weatherdatastore-bucket-11-07-2025-12-20-avar"
    destination_bucket = config_params.get("s3", {}).get("buckets", {}).get("source_bucket")
    source_prefix = config_params.get("s3", {}).get("buckets", {}).get("source_prefix")
    region = config_params.get("aws", {}).get("region", "us-east-1")
    s3Operations = S3Operations(source_bucket, region)
    dynamodb= DynamoDBOperations(region)
    try:
        s3_object_list = s3Operations.list_all_objects(
                source_prefix=source_prefix, extension='json'
        )

        for obj in s3_object_list:
            parts = obj.split("/")
            year_part, month_part , day_part,zip_code_part = parts[4], parts[5], parts[6],parts[7]
            country_code_part="US"
            zip_code= zip_code_part.split("=")[1]
            file_name = parts[-1]
            source_s3_key = (
                f"{source_prefix}/{year_part}/"
                f"{month_part}/{day_part}/"
                f"{zip_code_part}/{file_name}"
                )
            destination_s3_key = (
                f"{source_prefix}/{year_part}/"
                f"{month_part}/{day_part}/"
                f"country_code={country_code_part}/"
                f"zip_code={zip_code}/{file_name}"
                )
            logger.info(f"source file {source_bucket}/{source_s3_key}")
            logger.info(f"target file {destination_bucket}/{destination_s3_key}")
            s3Operations.copy_s3_key(
                source_bucket_name= source_bucket,
                source_object_key= source_s3_key,
                destination_bucket_name= destination_bucket,
                destination_object_key=destination_s3_key
                )
            logger.info(f"Copied {source_bucket}/{source_s3_key} to {destination_bucket}/{destination_s3_key}")
    except Exception as e:
        logger.error(f"Error repartioning objects in {source_bucket}\{source_prefix}{str(e)}", exc_info=True)
        raise
if __name__=="__main__":
    repartition_s3_objects()