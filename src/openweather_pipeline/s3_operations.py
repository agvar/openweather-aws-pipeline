import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime

import pandas as pd
from typing import List, Dict, Any, Optional, cast
from openweather_pipeline.logger import get_logger

logger = get_logger(__name__)


class S3Operations:
    def __init__(self, bucket: str, region: str) -> None:
        logger.info(f"Initializing S3Operations for bucket: {bucket}, region: {region}")
        self.s3_client = boto3.client("s3", region_name=region)
        self.bucket = bucket
        self._validate_bucket()
        logger.info("S3Operations validated successfully")

    def _validate_bucket(self) -> None:
        logger.info(f"Validate bucket: {self.bucket}")
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket exists and is readable {self.bucket}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"Bucket verification failed. Code: {error_code}", exc_info=True)
            if error_code == "404":
                raise ValueError(f"Bucket '{self.bucket}' does not exist")
            elif error_code == "403":
                raise ValueError(f"Access denied to bucket '{self.bucket}'")
            elif error_code == "NoSuchBucket":
                raise ValueError(f"Bucket '{self.bucket}' not found")
            else:
                raise ValueError(f"Error accessing bucket: {error_code}")
        except Exception as e:
            logger.error("Bucket verification failed", exc_info=True)
            raise ValueError(f"Unexpected Error :{e}")

    def read_file_as_bytes(self, key: str) -> bytes:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = cast(bytes, response["Body"].read())
            return content
        except ClientError as e:
            logger.error(f"S3 Client Error for {key} :{str(e)}", exc_info=True)
            raise ValueError(f"Could not read {key} from s3:{str(e)}")
        except Exception as e:
            logger.error(f"Failed to read object in S3: {str(e)}", exc_info=True)
            raise

    def store_object_in_s3(self, key: str, body: str, transfer: str = 'put') -> str:
        try:
            timestamp = datetime.now()
            logger.info(f"Storing object in S3: s3://{self.bucket}/{key}")
            
            if transfer=='upload_fileobj':
                response= self.s3_client.upload_fileobj(
                    Fileobj = body,
                    Bucket=self.bucket,
                    Key=key,
                    ExtraArgs={
                        "ContentType": "application/json",
                        "Metadata": {
                        "collection_time": timestamp.isoformat(),
                        "source": "Openweather_api_response",
                        }
                    }
                )
            else:
                response = self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                    Metadata={
                        "collection_time": timestamp.isoformat(),
                        "source": "Openweather_api_response",
                    },
                                )



            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info(f"Successfully stored data for key {key} in S3")
                return key
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

    def read_and_save_json_files_to_parquet(
        self, source_prefix: str, target_prefix: str, target_file: str
    ) -> pd.DataFrame:
        logger.info("Starting loading of JSON files from s3://{self.bucket}")
        all_data: List[pd.DataFrame] = []
        try:
            if not source_prefix.endswith("/"):
                source_prefix += "/"
            logger.info(f"Searching for JSON files under {self.bucket}/{source_prefix}...")

            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=source_prefix)
            for page in pages:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not key.lower().endswith(".json") or key.endswith("/"):
                        continue
                    logger.info(f"Reading JSON file into dataframe for key {key}")
                    response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                    content = response["Body"].read().decode("utf-8")
                    data = json.loads(content)
                    data_flattened = self.flatten_data(key, data)

                    key_parts = key.split("/")
                    zip_code_part = key_parts[-2]
                    country_code_part = key_parts[-3]
                    zip_code = zip_code_part.split("=")[1]
                    country_code = country_code_part.split("=")[1]
                    data_flattened["zip_code"] = zip_code
                    data_flattened["country_code"] = country_code
                    logger.info(f"Completed reading JSON file into dataframe for key {key}")
                    all_data.append(data_flattened)

            if not all_data:
                logger.error(
                    f"No data loaded for folder: {source_prefix} in bucket {self.bucket}",
                    exc_info=True,
                )
                raise ValueError(
                    f"No data loaded for folder: {source_prefix} in bucket {self.bucket}"
                )

            df = pd.DataFrame(all_data)
            logger.info(f"Dataframe created with shape {df.shape}")
            df.to_parquet(f"s3://{self.bucket}/{target_prefix}/{target_file}")
            logger.info(f"Saved dataframe to s3://{self.bucket}/{target_prefix}/{target_file}")

        except Exception as e:
            logger.error(f"Failed to read JSON file {key}: {str(e)}", exc_info=True)
            raise

    def flatten_data(self, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"starting flattening of JSON file ,key {key}")
        try:
            flattened = {
                "date": data.get("date"),
                "cloud_cover": data.get("cloud_cover", {}).get("afternoon"),
                "humidity": data.get("humidity", {}).get("afternoon"),
                "precipitation": data.get("precipitation", {}).get("total"),
                "pressure": data.get("pressure", {}).get("afternoon"),
                "temperature": data.get("temperature", {}).get("afternoon"),
                "temperature_min": data.get("temperature", {}).get("min"),
                "temperature_max": data.get("temperature", {}).get("max"),
                "wind_speed": data.get("wind", {}).get("max", {}).get("speed"),
                "wind_direction": data.get("wind", {}).get("max", {}).get("direction"),
            }
            logger.info(f"flattening of JSON file ,key {key} completed")
            return flattened
        except Exception as e:
            logger.error(f"Failed to flatten JSON object{key}: {str(e)}", exc_info=True)
            raise

    def list_all_objects(self, source_prefix: str, extension: str) -> Optional[List[str]]:
        logger.info("Starting listing of all files from s3://{self.bucket}")
        all_data = []
        try:
            if not source_prefix.endswith("/"):
                source_prefix += "/"
            logger.info(f"Searching for .{extension} files under {self.bucket}/{source_prefix}...")
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=source_prefix)
            for page in pages:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    uri: str = f"s3://{self.bucket}/{key}"
                    if not key.lower().endswith(f".{extension}") or key.endswith("/"):
                        continue
                    logger.info(f"file added to list for key {key}")
                    all_data.append(uri)
            if not all_data:
                logger.error(
                    f"No files listed under folder: {source_prefix} in bucket {self.bucket}",
                    exc_info=True,
                )
                raise ValueError(
                    f"No files listed under folder: {source_prefix} in bucket {self.bucket}"
                )
            return all_data
        except Exception as e:
            logger.error(f"Failed to list objects under {self.bucket}, {e}", exc_info=True)
            raise

    def copy_s3_key(
        self,
        source_bucket_name: str,
        source_object_key: str,
        destination_bucket_name: str,
        destination_object_key: str,
    ) -> None:
        copy_source = {"Bucket": source_bucket_name, "Key": source_object_key}
        try:
            self.s3_client.copy_object(
                CopySource=copy_source, Bucket=destination_bucket_name, Key=destination_object_key
            )
            logger.info(
                f"file {source_bucket_name}/{source_object_key} copied "
                f"to {destination_bucket_name}/{destination_object_key}"
            )
        except Exception as e:
            logger.error(f"Failed to list objects under {self.bucket}, {e}", exc_info=True)
            raise
