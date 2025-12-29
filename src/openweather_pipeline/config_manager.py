from typing import Dict, Optional, Any
from openweather_pipeline.logger import get_logger
import os
import yaml
import boto3

logger = get_logger(__name__)


class ConfigManager:
    _instance: Optional["ConfigManager"] = None

    def __init__(self) -> None:
        config_path = self._get_config_path()
        self.config = self._load_config(config_path)
        self._api_key: Optional[str] = None
        self.ssm = boto3.client("ssm")

    @classmethod
    def _get_instance(cls) -> "ConfigManager":
        if cls._instance is not None:
            return cls._instance
        else:
            cls._instance = cls()
            return cls._instance

    @property
    def api_key(self) -> str:
        if self._api_key is None:
            self._api_key = self._get_api_key()
        return self._api_key

    def _get_api_key(self) -> str:
        response = self.ssm.get_parameter(Name="Openweatherapi_key", WithDecryption=True)
        api_key = response["Parameter"]["Value"]
        if not isinstance(api_key, str):
            raise ValueError(f"Expected string from parameter store , got {type(api_key)}")
        if not api_key:
            logger.error("Weather API key not found")
            raise ValueError("Weather API key not found")
        return api_key

    @staticmethod
    def _get_config_path() -> Any:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # check parent dir for local runs
        config_path = os.path.join(script_dir, "..", "config", "config.yaml")
        if os.path.exists(config_path):
            return config_path
        # check sibling dir for aws lambda run
        config_path = os.path.join(script_dir, "config", "config.yaml")
        if os.path.exists(config_path):
            return config_path

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path) as cfile:
                config = yaml.safe_load(cfile)
                if not isinstance(config, dict):
                    logger.error("Configuration file must be Dictionary")
                    raise ValueError("Configuration file msut be Dictionary")
                for section, section_vals in config.items():
                    if not isinstance(section_vals, dict):
                        logger.error(f"Configuration {section} section must be Dictionary")
                        raise ValueError(f"Configuration {section} section must be Dictionary")
                return config
        except Exception as e:
            raise e


def get_config() -> "ConfigManager":
    return ConfigManager._get_instance()
