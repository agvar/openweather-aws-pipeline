from WeatherDataCollector import WeatherDataCollector
import json
from typing import Dict,Any


def lambda_handler(event, context)-> Dict[str,Any]:
    try:
        weatherCollector = WeatherDataCollector()
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Weather data collection complete"}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": e, "request_id": context.request_id}),
        }
