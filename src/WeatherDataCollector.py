from APIManager import APIManager
from S3Operations import S3Operations
import yaml
import json
from typing import Dict, Any
import boto3
import os
from datetime import datetime,timedelta

class WeatherDataCollector:
    def __init__(self) -> None:
        try:
            self.ssm = boto3.client("ssm")
            self.config_path = self._get_config_path()
            self.api_key = self._get_api_key()
            self.config = self.__setup_config(self.config_path)
            self.geocoding_url = self.config.get("app", {}).get("geocoding_by_zipcode_url")
            self.weather_url_day = self.config.get("app", {}).get("weather_url_day")
            self.weather_historical_flag = self.config.get("app",{}).get("weather_historical_flag")
            self.weather_year_start = self.config.get("app", {}).get("weather_year_start")
            self.weather_year_end = self.config.get("app", {}).get("weather_year_end")
            self.country_code = self.config.get("app", {}).get("ISO3166_code")
            self.zipcodes = self.config.get("app", {}).get("zipcodes")
            self.header_user_agent = self.config.get("app", {}).get("header_user_agent")
            self.header_accept = self.config.get("app", {}).get("header_accept")
            self.source_bucket = self.config.get("s3", {}).get("buckets", {}).get("source_bucket")
            self.region = self.config.get("aws", {}).get("region", "us-east-1")
            self.TIMEOUT = 10
            self.apiManager = APIManager(self.header_user_agent, self.header_accept)
            self.s3Operations = S3Operations(self.source_bucket, self.region)
            self.API_DAILY_LIMIT= 10
        except Exception:
            raise

    def _get_config_path(self) -> Any:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # check parent dir for local runs
        config_path = os.path.join(script_dir, "..", "config", "config.yaml")
        if os.path.exists(config_path):
            return config_path
        # check sibling dir for aws lambda run
        config_path = os.path.join(script_dir, "config", "config.yaml")
        if os.path.exists(config_path):
            return config_path
    
    def __setup_config(self, config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path) as cfile:
                config = yaml.safe_load(cfile)
                if not isinstance(config, dict):
                    raise ValueError("Configuration file msut be Dictionary")
                for section, section_vals in config.items():
                    if not isinstance(section_vals, dict):
                        raise ValueError(f"Configuration {section} section must be Dictionary")
                return config
        except Exception as e:
            raise e

    def _get_api_key(self) -> str:
        response = self.ssm.get_parameter(Name="Openweatherapi_key", WithDecryption=True)
        api_key = response["Parameter"]["Value"]
        if not isinstance(api_key, str):
            raise ValueError(f"Expected string from parameter store , got {type(api_key)}")
        if not api_key:
            raise ValueError("Weather API key not found")
        return api_key

    def collect_weather_data(self) -> None:
        try:
            for zipcode in self.zipcodes:
                if not zipcode or not self.country_code:
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
                    raise ValueError(
                        "Latitude or Longitude not received from url response :{response_geo_json}"
                    )
                if self.weather_historical_flag:
                    start_day = datetime(self.weather_year_start,1,1)
                    end_day = datetime(self.weather_year_end,12,31)
                    current_day = start_day
                    while current_day <= end_day:
                        self._create_api_request(self,lat,lon,current_day,zipcode)
                        current_day += timedelta(days=1)
                else:
                    self._create_api_request(self,lat,lon,datetime.now()-timedelta(days=1),zipcode)
        except Exception:
            raise

    def _create_api_request(self,lat,lon,day,zipcode):
        try:
            weather_params = {
                            "lat": lat,
                            "lon": lon,
                            "date": day.strftime('%Y-%m-%d'),
                            "units": "imperial",
                            "lang": "en",
                            "appid": self.api_key,
            }
            weather_response = self.apiManager.API_get(
                            self.weather_url_day, weather_params, self.TIMEOUT
            )
            weather_json_response = self.apiManager.API_parse_json(weather_response)
            response_date = weather_json_response.get('date')
            if datetime.strptime(response_date,'%Y-%m-%d'):
                year,month,day = response_date.split("-")
                self.s3Operations.store_object_in_s3(
                    self.source_bucket, zipcode,year,month,day,json.dumps(weather_json_response)
                )
            else:
                raise ValueError("Weather API response does not return date key or the format{response_date} is incorrect")
        except Exception:
            raise

if __name__ == "__main__":
    weather_app = WeatherDataCollector()
    weather_app.collect_weather_data()
