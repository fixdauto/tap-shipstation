# CUSTOM PLUGIN: ShipStation Client with v2 API Support
#
# CHANGES FOR API v2 IN THIS PLUGIN:
# 1. Use ShipStation API v2 base URL (api.shipstation.com/v2/)
# 2. Support two auth modes:
#    - Header-based (default): send API key in headers ("api-key" and "SS-API-KEY")
#    - Basic Auth (optional): if api_secret is provided or auth_mode=="basic"
# 3. Keep robust error handling and pagination logic
#
# ERROR HANDLING FIXES (from v1):
# 1. Added try/catch around response.json() to handle HTML error responses
# 2. Added logging when JSON parsing fails
# 3. Added specific 401/403 error handling with clear messages
# 4. Added response content logging for debugging

import time
import requests
import pendulum
import singer
import json

LOGGER = singer.get_logger()
BASE_URL = 'https://api.shipstation.com/v2/'  # V2 API URL
PAGE_SIZE = 100


def prepare_datetime(dt):
    # ShipStation requests must be in Pacific timezone
    timezone = pendulum.timezone('America/Los_Angeles')
    converted = timezone.convert(dt).strftime('%Y-%m-%d %H:%M:%S')
    return converted


class ShipStationClient:
    def __init__(self, config):
        # V2 API supports both header-based key auth and Basic Auth
        self.api_key = config['api_key']
        self.api_secret = config.get('api_secret')
        self.auth_mode = config.get('auth_mode', 'header').lower()

    def make_request(self, url, params):
        LOGGER.info('Making request to %s with query parameters %s', url, params)
        params['page_size'] = PAGE_SIZE  # V2 API uses page_size instead of pageSize

        headers = {
            'Content-Type': 'application/json'
        }

        # Decide auth method
        if self.auth_mode == 'basic' or self.api_secret:
            # Basic Auth using api_key as username and api_secret as password
            response = requests.get(url, params=params, headers=headers, auth=(self.api_key, self.api_secret))
        else:
            # Header-based key auth (as seen working in Postman for some accounts)
            headers.update({
                'api-key': self.api_key,
                'SS-API-KEY': self.api_key
            })
            response = requests.get(url, params=params, headers=headers)
        return response

    def paginate(self, endpoint, params):
        url = BASE_URL + endpoint
        while True:
            response = self.make_request(url, params)
            headers = response.headers
            status_code = response.status_code
            LOGGER.info('ShipStation v2 %s request -> status %s (page=%s, page_size=%s)', endpoint, status_code, params.get('page'), params.get('page_size'))

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
                    if 'text/html' in response.headers.get('content-type', ''):
                        LOGGER.error('Received HTML response instead of JSON. This usually indicates an API error or authentication issue.')
                        if 'error' in response.text.lower() or 'unauthorized' in response.text.lower():
                            LOGGER.error('API response suggests authentication or authorization error.')
                    raise e
                except json.JSONDecodeError as e:
                    LOGGER.error('JSON decode error. Response status: %s', status_code)
                    LOGGER.error('Response headers: %s', dict(headers))
                    LOGGER.error('Response content (first 1000 chars): %s', response.text[:1000])
                    if 'text/html' in response.headers.get('content-type', ''):
                        LOGGER.error('Received HTML response instead of JSON. This usually indicates an API error or authentication issue.')
                        if 'error' in response.text.lower() or 'unauthorized' in response.text.lower():
                            LOGGER.error('API response suggests authentication or authorization error.')
                    raise e

                if response_json.get('total') == 0:
                    LOGGER.info('No Data for endpoint')
                    break
                # Items list can be addressed by endpoint name (e.g., 'shipments')
                items = response_json.get(endpoint, [])
                yield items
                LOGGER.info(
                    'Finished requesting page %s out of %s total pages.',
                    response_json.get('page'),
                    response_json.get('pages'))

                # Determine if more pages are available
                has_more = False
                if 'page' in response_json and 'pages' in response_json:
                    has_more = response_json['page'] < response_json['pages']
                elif 'links' in response_json:
                    next_link = response_json['links'].get('next') if isinstance(response_json['links'], dict) else None
                    has_more = bool(next_link)
                else:
                    has_more = len(items) == params.get('page_size', PAGE_SIZE)

                if not has_more:
                    break

                params['page'] = int(params.get('page', 1)) + 1

                remaining = None
                reset = None
                for k, v in headers.items():
                    lk = k.lower()
                    if 'rate-limit-remaining' in lk:
                        try:
                            remaining = int(v)
                        except Exception:
                            remaining = None
                    if 'rate-limit-reset' in lk:
                        try:
                            reset = int(v)
                        except Exception:
                            reset = None
                if remaining is not None and remaining < 1 and reset is not None:
                    wait_seconds = reset + 1
                    LOGGER.info("Waiting for %s seconds to respect ShipStation's API rate limit.", wait_seconds)
                    time.sleep(wait_seconds)
            elif status_code == 401:
                if self.auth_mode == 'basic' or self.api_secret:
                    LOGGER.error('Authentication failed (401). Check API key and secret or account permissions.')
                else:
                    LOGGER.error('Authentication failed (401). Header-based API key was rejected. Verify key or try auth_mode="basic" with api_secret if your account requires it.')
                response.raise_for_status()
            elif status_code == 403:
                LOGGER.error('Forbidden. Please check your API permissions.')
                response.raise_for_status()
            elif status_code == 429:
                time.sleep(60)
                LOGGER.info("Waiting for 60 seconds due to 429 without warning")
            else:
                LOGGER.error('Request failed with status %s', status_code)
                LOGGER.error('Response content: %s', response.text[:1000])
                response.raise_for_status()
