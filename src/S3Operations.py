import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid

class S3Operations():
    def __init__(self,bucket,region):
        self.s3_client = boto3.client('s3',region_name=region)
        self._validate_bucket(bucket)
        
    def _validate_bucket(self,bucket):
        try:
            self.s3_client.head_bucket(Bucket=bucket)
            print("Bucket exists and is readable {bucket}")
        except ClientError as e:
            error_code = e.response['Error']['code']
            if error_code == '404':
                raise ValueError(f"Bucket '{bucket}' does not exist")
            elif error_code == '403':
                raise ValueError(f"Access denied to bucket '{bucket}'")
            elif error_code == 'NoSuchBucket':
                raise ValueError(f"Bucket '{bucket}' not found")
            else:
                raise ValueError(f"Error accessing bucket: {error_code}")
        except Exception as e:
            print(f"unable to access bucket:{bucket}")
            raise ValueError(f"Unexpected Error :{e}")
            
    
    def store_object_in_s3(self,bucket,partition,body):
        try:
            timestamp = datetime.now()
            # Create partition structure: weather/year=2025/month=10/day=21/
            s3_key = f"openweather_api/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/zipcode={partition}/{uuid.uuid4()}.json"
            response = self.s3_client.put_object(
                Bucket = bucket,
                Key= s3_key,
                Body = body,
                ContentType='application/json',
                Metadata={
                    'part_key':partition,
                    'collection_time':timestamp.isoformat(),
                    'source':'Openweather_api_response'
                }
        )
        
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Successfully uploaded to s3://{bucket}/{s3_key}")
                return s3_key
            else:
                raise ValueError(f"Upload failed with status: {response['ResponseMetadata']['HTTPStatusCode']}")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            raise ValueError(f"S3 upload failed: {error_code}")
        except Exception as e:
            raise ValueError(f"Unexpected upload error: {e}")