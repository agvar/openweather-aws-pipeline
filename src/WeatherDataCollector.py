from APIManager import APIManager
from S3Operations import S3Operations
import yaml
import json
from typing import Dict, Any
import boto3
import os


class WeatherDataCollector:
    def __init__(self):
        try:
            self.ssm = boto3.client("ssm")
            self.config_path= self._get_config_path()
            self.api_key = self._get_api_key()
            self.config = self.__setup_config(self.config_path)
            self.geocoding_url = self.config.get("app", {}).get("geocoding_by_zipcode_url")
            self.weather_url = self.config.get("app", {}).get("weather_url")
            self.country_code = self.config.get("app", {}).get("ISO3166_code")
            self.zipcodes = self.config.get("app", {}).get("zipcodes")
            self.header_user_agent = self.config.get("app", {}).get("header_user_agent")
            self.header_accept = self.config.get("app", {}).get("header_accept")
            self.source_bucket = self.config.get("s3", {}).get("buckets", {}).get("source_bucket")
            self.region = self.config.get("aws", {}).get("region", "us-east-1")
            self.TIMEOUT = 10
            apiManager = APIManager(self.header_user_agent, self.header_accept)
            s3Operations = S3Operations(self.source_bucket, self.region)

            for zipcode in self.zipcodes:
                if not zipcode or not self.country_code:
                    continue
                geo_params = {
                    "zip": f"{zipcode},{self.country_code}",
                    "appid": self.api_key,
                }
                geo_response = apiManager.API_get(self.geocoding_url, geo_params, self.TIMEOUT)
                geo_response_json = apiManager.API_parse_json(geo_response)

                lat = geo_response_json.get("lat")
                lon = geo_response_json.get("lon")
                if not lat or not lon:
                    raise ValueError(
                        "Latitude or Longitude not received from url response :{response_geo_json}"
                    )
                weather_params = {
                    "lat": lat,
                    "lon": lon,
                    "units": "imperial",
                    "lang": "en",
                    "appid": self.api_key,
                }
                weather_response = apiManager.API_get(
                    self.weather_url, weather_params, self.TIMEOUT
                )
                weather_json_response = apiManager.API_parse_json(weather_response)
                s3Operations.store_object_in_s3(
                    self.source_bucket, zipcode, json.dumps(weather_json_response)
                )
        except Exception:
            raise

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

    def _get_config_path(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir,"..","config","config.yaml")

    def _get_api_key(self) -> str:
        response = self.ssm.get_parameter(Name="Openweatherapi_key", WithDecryption=True)
        api_key = response["Parameter"]["Value"]
        print(f"api key:{api_key}")
        if not api_key:
            raise ValueError("Weather API key not found")
            exit(1)
        return api_key


if __name__ == "__main__":
    weather_app = WeatherDataCollector()
