import requests
from typing import Dict, List, Any


class APIManager:
    def __init__(self, header_user_agent: str, header_accept: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": header_user_agent, "Accept": header_accept})

    def API_get(self, url: str, params: Dict[str, Any], timeout: int) -> requests.Response:
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            raise

    def API_parse_json(self, response: requests.Response, keys: List[str] = []) -> Dict[str, Any]:
        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(f"Expected JSON object, got {type(response_json)}")

        if keys:
            return {key: response_json.get(key) for key in keys if key in response_json}
        else:
            return response_json
