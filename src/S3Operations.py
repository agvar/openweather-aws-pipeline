import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
from .logger_config import get_logger
logger = get_logger(__name__)


class S3Operations:
    def __init__(self, bucket: str, region: str) -> None:
        logger.info(f"Initializing S3Operations for bucket: {bucket}, region: {region}")
        self.s3_client = boto3.client("s3", region_name=region)
        self._validate_bucket(bucket)
        logger.info("S3Operations validated successfully")

    def _validate_bucket(self, bucket: str) -> None:
        logger.info(f"Validate bucket: {bucket}")
        try:
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(f"Bucket exists and is readable {bucket}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"Bucket verification failed. Code: {error_code}", exc_info=True)
            if error_code == "404":
                raise ValueError(f"Bucket '{bucket}' does not exist")
            elif error_code == "403":
                raise ValueError(f"Access denied to bucket '{bucket}'")
            elif error_code == "NoSuchBucket":
                raise ValueError(f"Bucket '{bucket}' not found")
            else:
                raise ValueError(f"Error accessing bucket: {error_code}")
        except Exception as e:
            logger.error("Bucket verification failed", exc_info=True)
            raise ValueError(f"Unexpected Error :{e}")

    def store_object_in_s3(
            self,
            bucket: str,
            zipcode: str,
            year: str,
            month: str,
            day: str,
            body: str
            ) -> str:
        try:
            timestamp = datetime.now()
            s3_key = (
                f"openweather_api/year={year}/"
                f"month={month}/day={day}/"
                f"zipcode={zipcode}/{uuid.uuid4()}.json"
            )

            logger.info(f"Storing object in S3: s3://{bucket}/{s3_key}")
            response = self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=body,
                ContentType="application/json",
                Metadata={
                    "part_key": zipcode,
                    "collection_time": timestamp.isoformat(),
                    "source": "Openweather_api_response",
                },
            )

            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info(f"Successfully stored data for zipcode {zipcode} in S3")
                return s3_key
            else:
                logger.error(
                    f"S3 Upload failed with status: \
                        {response['ResponseMetadata']['HTTPStatusCode']}"
                    )
                raise ValueError(
                    f"Upload failed with status: {response['ResponseMetadata']['HTTPStatusCode']}"
                )

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(
                f"Failed to store object in S3. Error code: {str(error_code)}", exc_info=True
            )
            raise ValueError(f"S3 upload failed: {error_code}")
        except Exception as e:
            logger.error(f"Failed to store object in S3: {str(e)}", exc_info=True)
            raise ValueError(f"Unexpected upload error: {e}")
