import requests
from typing import Dict, List, Any
from logger import get_logger

logger = get_logger(__name__)


class APIManager:
    def __init__(self, header_user_agent: str, header_accept: str) -> None:
        logger.info("Initializing APIManager")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": header_user_agent, "Accept": header_accept})

    def API_get(self, url: str, params: Dict[str, Any], timeout: int) -> requests.Response:
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            logger.info(f"API GET request to url {url}")

            logger.info(f"API response status: {response.status_code}")
            response.raise_for_status()

            return response
        except requests.exceptions.Timeout as e:
            logger.error(f"API request failed: {str(e)}", exc_info=True)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}", exc_info=True)
            raise

    def API_parse_json(self, response: requests.Response, keys: List[str] = []) -> Dict[str, Any]:
        try:
            logger.info("Parsing API response as JSON")
            response_json = response.json()
            if not isinstance(response_json, dict):
                raise ValueError(f"Expected JSON object, got {type(response_json)}")

            if keys:
                logger.info(f"Filtering response fr keys: {keys}")
                return {key: response_json.get(key) for key in keys if key in response_json}
            else:
                return response_json
        except Exception as e:
            logger.error(f"Failed to parse JSON: {str(e)}", exc_info=True)
            raise
