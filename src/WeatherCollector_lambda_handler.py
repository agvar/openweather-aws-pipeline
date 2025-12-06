from WeatherDataCollector import WeatherDataCollector
import json
from typing import Dict, Any
import traceback


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        weatherCollector = WeatherDataCollector()
        weatherCollector.collect_weather_data()
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Weather data collection complete"}),
        }
    except Exception as e:
        print(f"Error : {str(e)}")
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": e, "request_id": context.request_id}),
        }
