from APIManager import APIManager
from S3Operations import S3Operations
import yaml
import json
import boto3
from datetime import datetime, timedelta
from logger import get_logger

logger = get_logger(__name__)


class WeatherDataCollector:
    def __init__(self) -> None:
        logger.info("Initializing WeatherDataCollector")
        try:
            self.ssm = boto3.client("ssm")
            self.config = get_config().config
            self.api_key = get_config().api_key
            self.geocoding_url = self.config.get("app", {}).get("geocoding_by_zipcode_url")
            self.weather_url_day = self.config.get("app", {}).get("weather_url_day")
            self.weather_historical_flag = self.config.get("app", {}).get("weather_historical_flag")
            self.weather_year_start = self.config.get("app", {}).get("weather_year_start")
            self.weather_year_end = self.config.get("app", {}).get("weather_year_end")
            self.country_code = self.config.get("app", {}).get("ISO3166_code")
            self.zipcodes = self.config.get("app", {}).get("zipcodes")
            self.header_user_agent = self.config.get("app", {}).get("header_user_agent")
            self.header_accept = self.config.get("app", {}).get("header_accept")
            self.source_bucket = self.config.get("s3", {}).get("buckets", {}).get("source_bucket")
            self.prefix = self.config.get("s3", {}).get("buckets", {}).get("source_prefix")
            self.region = self.config.get("aws", {}).get("region", "us-east-1")
            self.apiManager = APIManager(self.header_user_agent, self.header_accept)
            self.s3Operations = S3Operations(self.source_bucket, self.region)
            self.API_DAILY_LIMIT = 10
            self.TIMEOUT = 10

            logger.info("WeatherDataCollector initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize WeatherDataCollector {str(e)}", exc_info=True)
            raise

    def collect_weather_data(self) -> None:
        logger.info("Starting weather daya collection for all zipcdes")
        try:
            for idx, zipcode in enumerate(self.zipcodes):
                logger.info(f"Processing zipcode {idx+1} of {len(self.zipcodes)} : {zipcode}")
                if not zipcode or not self.country_code:
                    logger.warning(
                        f"Skipping invalid zipcode:{zipcode} or country code{self.country_code} "
                    )
                    continue

                geo_params = {
                    "zip": f"{zipcode},{self.country_code}",
                    "appid": self.api_key,
                }
                geo_response = self.apiManager.API_get(self.geocoding_url, geo_params, self.TIMEOUT)
                geo_response_json = self.apiManager.API_parse_json(geo_response)

                lat = geo_response_json.get("lat")
                lon = geo_response_json.get("lon")
                if not lat or not lon:
                    logger.error(f"Missing coordinates for zipcode {zipcode}: {geo_response_json}")
                    raise ValueError(
                        "Latitude or Longitude not received from url response :{response_geo_json}"
                    )
                if self.weather_historical_flag:
                    start_day = datetime(self.weather_year_start, 1, 1)
                    end_day = datetime(self.weather_year_end, 12, 31)
                    logger.info(
                        f"Starting Historical weather data collection from {start_day} to {end_day}"
                    )
                    current_day = start_day
                    while current_day <= end_day:
                        logger.info(f"Starting weather data collection for {current_day}")
                        self._process_api(lat, lon, current_day, zipcode)
                        logger.info(
                            f"Weather data collection for {current_day} completed successfully"
                        )
                        current_day += timedelta(days=1)
                else:
                    last_day = datetime.now() - timedelta(days=1)
                    logger.info(f"Starting last day weather data collection for {last_day}")
                    self._process_api(lat, lon, last_day, zipcode)
                    logger.info(f"Weather data collection for {last_day} completed successfully")

        except Exception as e:
            logger.error(f"Error weather data collection {str(e)}", exc_info=True)
            raise

    def _process_api(self, lat: str, lon: str, day: datetime, zipcode: str) -> None:
        try:
            logger.info(f"starting api processing for zipcode{zipcode} and day :{day}")
            weather_params = {
                "lat": lat,
                "lon": lon,
                "date": day.strftime("%Y-%m-%d"),
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
                logger.info(f"api processing complete for zipcode{zipcode} and day :{day}")
            else:
                raise ValueError(
                    f"Weather API response does not return date key, \
                    or the format{response_date} is incorrect"
                )

        except Exception as e:
            logger.error(f"Error during weather data collection: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    weather_app = WeatherDataCollector()
    weather_app.collect_weather_data()
