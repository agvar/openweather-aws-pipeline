import pytest
import requests
from unittest.mock import Mock,patch
import sys
from src.APIManager import APIManager

class TestAPIManager:

    def setup_method(self):
        self.header_user_agent="TestWeatherApp/1.0"
        self.header_accept="application/json"
        self.api_manager= APIManager(
            header_user_agent=self.header_user_agent,
            header_accept=self.header_accept
        )
    
    def test_init_creates_session_with_headers(self):
        assert self.api_manager.session.headers['User-Agent'] == self.header_user_agent
        assert self.api_manager.session.headers['Accept'] == self.header_user_agent
    
    @patch('requests.Session.get')
    def test_api_get_sucess(self,mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.api_manager.API_get(
            url="https://testapi.weather",
            params = {"q":"London"},
            timeout=10
        )
        assert result == mock_response
        mock_get.assert_called_once_with(
            url="https://testapi.weather",
            params = {"q":"London"},
            timeout=10
        )


