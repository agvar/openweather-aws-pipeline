import logging
import sys
import os
from typing import Optional


class Logger:
    _logger: Optional[logging.Logger] = None

    @classmethod
    def get_logger(cls, name: str = "WeatherDataCollector") -> logging.Logger:
        if cls._logger:
            return cls._logger
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, 'INFO'))
        if logger.handlers:
            return logger
        is_lambda = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None
        if is_lambda:
            handler: logging.Handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(levelname)s - %(name)s - %(message)s'
            )
        else:
            handler = logging.FileHandler('weather_data_collector.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
            )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        cls._logger = logger
        return logger


def get_logger(name: str = "WeatherDataCollector") -> logging.Logger:
    return Logger.get_logger(name)
