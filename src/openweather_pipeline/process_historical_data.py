from openweather_pipeline.s3_operations import S3Operations
from openweather_pipeline.logger import get_logger
from openweather_pipeline.config_manager import get_config

logger = get_logger(__name__)


class DataLoader:
    def __init__(self) -> None:
        logger.info("Initializing Historical data processing")
        try:
            self.config = get_config().config
            self.api_key = get_config().api_key
            self.source_bucket = self.config.get("s3", {}).get("buckets", {}).get("source_bucket")
            self.source_prefix = self.config.get("s3", {}).get("buckets", {}).get("source_prefix")
            self.processed_prefix = (
                self.config.get("s3", {}).get("buckets", {}).get("processed_prefix")
            )
            self.processed_file_name = (
                self.config.get("s3", {}).get("buckets", {}).get("processed_file_name")
            )
            self.region = self.config.get("aws", {}).get("region", "us-east-1")
            self.s3Operations = S3Operations(self.source_bucket, self.region)

            logger.info("Historical data processing initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Historical data processing {str(e)}", exc_info=True)
            raise

    def read_and_save_json_files_to_dataframe(self) -> None:
        logger.info("starting read of JSON files into dataframe")
        try:
            self.s3Operations.read_and_save_json_files_to_dataframe(
                self.source_prefix, self.processed_prefix, self.processed_file_name
            )
        except Exception as e:
            logger.error(f"Error during read of JSON files into dataframe: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    weather_app = DataLoader()
    weather_app.read_and_save_json_files_to_dataframe()
