from openweather_pipeline.APIManager import APIManager
from openweather_pipeline.S3Operations import S3Operations
import json
from datetime import datetime
from openweather_pipeline.logger import get_logger
from openweather_pipeline.config_manager import get_config
from typing import Dict, Tuple

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
            self.API_DAILY_LIMIT = 10
            self.TIMEOUT = 10
            self.zip_to_geocode_map: Dict[Tuple[str, str], Tuple[float, float]] = {}

            logger.info("WeatherDataCollector initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize WeatherDataCollector {str(e)}", exc_info=True)
            raise

    def collect_weather_data(self, zipcode: str, country_code: str, process_day: str) -> None:

        logger.info(f"Starting weather day collection for zipcode{zipcode} and day{process_day}")
        try:
            if self.zip_to_geocode_map.get((zipcode, country_code)) is None:
                geo_params = {
                    "zip": f"{zipcode},{country_code}",
                    "appid": self.api_key,
                }
                geo_response = self.apiManager.API_get(self.geocoding_url, geo_params, self.TIMEOUT)
                geo_response_json = self.apiManager.API_parse_json(geo_response)
                lat, lon = geo_response_json.get("lat"), geo_response_json.get("lon")
                self.zip_to_geocode_map[(zipcode, country_code)] = (lat, lon)
            else:
                lat, lon = self.zip_to_geocode_map[(zipcode, country_code)]
                logger.info(f"Re-using calculated geocode for {zipcode} :{(lat,lon)}")

            if not lat or not lon:
                logger.error(f"Missing coordinates for zipcode {zipcode}: {geo_response_json}")
                raise ValueError(
                    "Latitude or Longitude not received from url response :{response_geo_json}"
                )
            logger.info(f"starting weather api for zipcode{zipcode} and day :{process_day}")
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
                    zipcode,
                    response_year,
                    response_month,
                    response_day,
                    json.dumps(weather_json_response),
                )
                logger.info(f"api processing complete for zipcode{zipcode} and day :{response_day}")
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
