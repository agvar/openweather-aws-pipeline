import requests
import os
import json

class APIManager:
    def __init__(self,header_user_agent,header_accept):      
            self.session = requests.Session()
            self.session.headers.update(
                {'User-Agent':header_user_agent,
                'Accept':header_accept}
            )
    def API_get(self,url,params,timeout):
        try:
                response = self.session.get(url,params=params,timeout=timeout)
                response.raise_for_status()
                return response
        except (requests.exceptions.RequestException) as e:
            raise

            
    def API_parse_json(self,response,keys=[]):
        response = response.json()
        response_dict = {}
        if keys:
            for key in keys :
                if key in response:
                    response_dict.update({key:response.get(key)})
        else:
            return response
        return response_dict
