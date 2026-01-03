from openweather_pipeline.api_manager import APIManager
from openweather_pipeline.s3_operations import S3Operations
from openweather_pipeline.dynamodb_operations import DynamoDBOperations
import json
from datetime import datetime
from openweather_pipeline.logger import get_logger
from openweather_pipeline.config_manager import get_config
from openweather_pipeline.models.collection_models import CollectionGeocodeCache
from decimal import Decimal

logger = get_logger(__name__)


class WeatherDataCollector:
    def __init__(self) -> None:
        logger.info("Initializing WeatherDataCollector")
        try:
            self.config = get_config().config
            self.api_key = get_config().api_key
            self.geocoding_url = self.config.get("app", {}).get("geocoding_by_zipcode_url")
            self.weather_url_day = self.config.get("app", {}).get("weather_url_day")
            self.header_user_agent = self.config.get("app", {}).get("header_user_agent")
            self.header_accept = self.config.get("app", {}).get("header_accept")
            self.source_bucket = self.config.get("s3", {}).get("buckets", {}).get("source_bucket")
            self.prefix = self.config.get("s3", {}).get("buckets", {}).get("source_prefix")
            self.region = self.config.get("aws", {}).get("region", "us-east-1")
            self.apiManager = APIManager(self.header_user_agent, self.header_accept)
            self.s3Operations = S3Operations(self.source_bucket, self.region)
            self.dynamodb = DynamoDBOperations(self.region)
            self.TIMEOUT = 10
            self.geocode_cache_table = (
                self.config.get("dynamodb", {}).get("tables", {}).get("geocode_cache_table")
            )

            logger.info(f"WeatherDataCollector initialized successfully.{self.geocode_cache_table}")
        except Exception as e:
            logger.error(f"Failed to initialize WeatherDataCollector {str(e)}", exc_info=True)
            raise

    def collect_weather_data(self, zip_code: str, country_code: str, process_day: str) -> None:

        logger.info(f"Starting geocoding for zipcode{zip_code} country_code{country_code}")
        try:
            geocode_item = self.dynamodb.get_item(
                model_class=CollectionGeocodeCache,
                table_nm=self.geocode_cache_table,
                key={"zip_code": zip_code, "country_code": country_code},
            )
            if geocode_item:
                lat, lon = geocode_item.latitude, geocode_item.longitude
            else:
                geo_params = {
                    "zip": f"{zip_code},{country_code}",
                    "appid": self.api_key,
                }
                geo_response = self.apiManager.API_get(self.geocoding_url, geo_params, self.TIMEOUT)
                geo_response_json = self.apiManager.API_parse_json(geo_response)

                if geo_response_json is None:
                    logger.error(
                        f"Missing geocode response for zipcode {zip_code}: {geo_response_json}"
                    )
                    raise ValueError(
                        "Missing geocode response for zipcode {zip_code},"
                        " country_cde {country_code}"
                    )
                else:
                    lat_response = geo_response_json.get("lat")
                    lon_response = geo_response_json.get("lon")
                    name_response = geo_response_json.get("name")
                    country_response = geo_response_json.get("name")

                if lat_response is not None and lon_response is not None:
                    geocode_record = CollectionGeocodeCache(
                        zip_code=zip_code,
                        country_code=country_code,
                        latitude=lat_response,
                        longitude=lon_response,
                        name=name_response,
                        country=country_response,
                    )
                    self.dynamodb.put_item(
                        model_instance=geocode_record,
                        table_nm=self.geocode_cache_table,
                    )
                    logger.info(f" (lat, lon):{(zip_code, country_code)}")
                    lat, lon = Decimal(lat_response), Decimal(lon_response)
                else:
                    logger.error(f"Missing coordinates for zipcode {zip_code}: {geo_response_json}")
                    raise ValueError(
                        "Latitude or Longitude not received from url response :{response_geo_json}"
                    )

            logger.info(
                f"starting weather api for zipcode{zip_code},"
                f"country_cde {country_code} and day :{process_day}"
            )
            weather_params = {
                "lat": lat,
                "lon": lon,
                "date": process_day,
                "units": "imperial",
                "lang": "en",
                "appid": self.api_key,
            }
            weather_response = self.apiManager.API_get(
                self.weather_url_day, weather_params, self.TIMEOUT
            )
            weather_json_response = self.apiManager.API_parse_json(weather_response)
            response_date = weather_json_response.get("date")

            if not isinstance(response_date, str):
                logger.info(f"Invalid date format in reponse:{response_date}")
                raise ValueError(f"Expected string, got {type(response_date)}")

            if datetime.strptime(response_date, "%Y-%m-%d"):
                response_year, response_month, response_day = response_date.split("-")
                self.s3Operations.store_object_in_s3(
                    self.prefix,
                    zip_code,
                    response_year,
                    response_month,
                    response_day,
                    json.dumps(weather_json_response),
                )
                logger.info(
                    f"api processing complete for zipcode{zip_code}, \
                    country_code{country_code} and day :{response_day}"
                )
            else:
                raise ValueError(
                    f"Weather API response does not return date key, \
                    or the format{response_date} is incorrect"
                )
            logger.info(f"Weather data collection for {process_day} completed successfully")
        except Exception as e:
            logger.error(f"Error weather data collection {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    weather_app = WeatherDataCollector()
    weather_app.collect_weather_data("10001", "US", "2025-12-16")
