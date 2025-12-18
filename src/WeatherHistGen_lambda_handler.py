from config_manager import get_config
from typing import Dict, Any, List
from logger import get_logger
from datetime import datetime, timedelta

logger = get_logger(__name__)


def histGen_lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    config_parms = get_config().config
    weather_historical_flag: bool = config_parms.get("app", {}).get("weather_historical_flag")
    weather_year_start: int = int(config_parms.get("app", {}).get("weather_year_start"))
    weather_year_end: int = int(config_parms.get("app", {}).get("weather_year_end"))
    zipcodes: List[Dict[str, Any]] = config_parms.get("app", {}).get("zipcodes")
    weather_days: Dict[str, Any] = {"days": []}

    year_start = datetime(weather_year_start, 1, 1)
    year_end = datetime(weather_year_end, 12, 31)
    yesterday = datetime.now() - timedelta(days=1)

    if not zipcodes:
        logger.error("No zipcodes to process")
        raise ValueError("No zipcodes to process in config file ")

    for item in zipcodes:
        zipcode, country_code = item.get("zipcode"), item.get("country_code")
        if zipcode is None or country_code is None:
            logger.error(f"Zipcode{zipcode} or contry code{country_code} is missing")
            raise ValueError(f"Zipcode{zipcode} or contry code{country_code} is missing")
        if weather_historical_flag:
            process_day = year_start
            while process_day <= year_end:
                weather_days["days"].append(
                    {
                        "zipcode": zipcode,
                        "country_code": country_code,
                        "process_day": process_day.strftime("%Y-%m-%d"),
                    }
                )
                process_day += timedelta(days=1)
        else:
            weather_days["days"].append(
                {
                    "zipcode": zipcode,
                    "country_code": country_code,
                    "process_day": yesterday.strftime("%Y-%m-%d"),
                }
            )
    if len(weather_days["days"]) <= 0:
        logger.error("No input for Weather data Generation")
        raise ValueError(" No zipcodes to process in config file ")
    return weather_days
