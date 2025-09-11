# CUSTOM PLUGIN: ShipStation Client with Error Handling
#
# MAIN FIXES:
#   1. Added try/catch around response.json() to handle HTML error responses 
#   2. Added logging when JSON parsing fails
#   3. Added specific 401/403 error handling with clear messages
#   4. Added response content logging for debugging
#
# The original client.py crashed at line 32: response_json = response.json()
# when the API returned "401 Unauthorized" as plain text instead of JSON

import time
import requests
import pendulum
import singer
import json

LOGGER = singer.get_logger()
BASE_URL = 'https://ssapi.shipstation.com/'
PAGE_SIZE = 100

def prepare_datetime(dt):
    # ShipStation requests must be in Pacific timezone
    timezone = pendulum.timezone('America/Los_Angeles')
    converted = timezone.convert(dt).strftime('%Y-%m-%d %H:%M:%S')
    return converted

class ShipStationClient:
    def __init__(self, config):
        self.username = config['api_key']
        self.password = config['api_secret']

    def make_request(self, url, params):
        LOGGER.info('Making request to %s with query parameters %s', url, params)
        params['pageSize'] = PAGE_SIZE
        response = requests.get(url, params=params, auth=(self.username, self.password))
        return response

    def paginate(self, endpoint, params):
        url = BASE_URL + endpoint        
        while True:
            response = self.make_request(url, params)
            headers = response.headers
            status_code = response.status_code
            
            if status_code == 200:
                # CUSTOM FIX: Added try/catch around response.json() to handle HTML error responses
                # ORIGINAL CODE: response_json = response.json()  # This line crashed when API returned HTML
                try:
                    response_json = response.json()
                except requests.exceptions.JSONDecodeError as e:
                    # CUSTOM FIX: Log detailed debugging info instead of just crashing
                    LOGGER.error('JSON decode error. Response status: %s', status_code)
                    LOGGER.error('Response headers: %s', dict(headers))
                    LOGGER.error('Response content (first 1000 chars): %s', response.text[:1000])
                    # Try to extract more detailed error information
                    if 'text/html' in response.headers.get('content-type', ''):
                        LOGGER.error('Received HTML response instead of JSON. This usually indicates an API error or authentication issue.')
                        if 'error' in response.text.lower() or 'unauthorized' in response.text.lower():
                            LOGGER.error('API response suggests authentication or authorization error.')
                    raise e
                except json.JSONDecodeError as e:
                    # CUSTOM FIX: Handle older Python versions that use json.JSONDecodeError
                    LOGGER.error('JSON decode error. Response status: %s', status_code)
                    LOGGER.error('Response headers: %s', dict(headers))
                    LOGGER.error('Response content (first 1000 chars): %s', response.text[:1000])
                    if 'text/html' in response.headers.get('content-type', ''):
                        LOGGER.error('Received HTML response instead of JSON. This usually indicates an API error or authentication issue.')
                        if 'error' in response.text.lower() or 'unauthorized' in response.text.lower():
                            LOGGER.error('API response suggests authentication or authorization error.')
                    raise e
                
                if response_json['total'] == 0:
                    LOGGER.info('No Data for endpoint')
                    break
                yield response_json[endpoint]
                LOGGER.info(
                    'Finished requesting page %s out of %s total pages.',
                    response_json['page'],
                    response_json['pages'])
                if response_json['page'] >= response_json['pages']:
                    break
                params['page'] += 1

                # Respect API rate limits
                if int(headers['X-Rate-Limit-Remaining']) < 1:
                    wait_seconds = int(headers['X-Rate-Limit-Reset']) + 1 # Buffer of 1 second
                    LOGGER.info(
                        "Waiting for %s seconds to respect ShipStation's API rate limit.",
                        wait_seconds)
                    time.sleep(wait_seconds)
            elif status_code == 401:
                # CUSTOM FIX: Added specific 401 error handling with clear message
                LOGGER.error('Authentication failed. Please check your API key and secret.')
                response.raise_for_status()
            elif status_code == 403:
                # CUSTOM FIX: Added specific 403 error handling with clear message
                LOGGER.error('Forbidden. Please check your API permissions.')
                response.raise_for_status()
            elif status_code == 429:
                time.sleep(60)
                LOGGER.info("Waiting for 60 seconds due to 429 without warning")
            else:
                # CUSTOM FIX: Added response content logging for debugging other errors
                LOGGER.error('Request failed with status %s', status_code)
                LOGGER.error('Response content: %s', response.text[:1000])
                response.raise_for_status()