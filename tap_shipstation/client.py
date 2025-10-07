'''
Defines a client for interacting with the ShipStation v2 API.

The `ShipStationClient` class encapsulates the logic for:
  - Authenticating requests using an API key in the headers.
  - Making GET requests to specified API endpoints.
  - Automatically handling pagination to retrieve all records from a resource.
  - Respecting API rate limits by pausing execution when necessary.
  -  Error handling for common HTTP issues and unexpected response formats (e.g., HTML errors).
Designed to be used as part of a Singer tap for extracting data from ShipStation.
'''

import time
import requests
import pendulum
import singer
import json

# Singer logger for consistent, structured logs
LOGGER = singer.get_logger()
BASE_URL = 'https://api.shipstation.com/v2/'  # V2 API URL
PAGE_SIZE = 100
LEGACY_BASE_URLS = [
    'https://api.shipstation.com/',          # attempted earlier (did not work)
    'https://ssapi.shipstation.com/',        # classic ShipStation documented base
]
ALT_ENGINE_BASE_URLS = [
    'https://api.shipengine.com/v1/',        # ShipEngine v1 base (orders may live here)
]


def prepare_datetime(dt):
    # Helper: convert any datetime to the ShipStation-required timezone/format.
    # Note: This function is currently unused by the sync loop but kept for
    # clarity and potential future use (e.g., if ShipStation accepts timestamps).
    # ShipStation requests must be in Pacific timezone
    timezone = pendulum.timezone('America/Los_Angeles')
    converted = timezone.convert(dt).strftime('%Y-%m-%d %H:%M:%S')
    return converted


class ShipStationClient:
    # Thin API client responsible for:
    # - Injecting header-based authentication (v2 API)
    # - Making GET requests with consistent pagination params
    # - Providing a paginate() generator that yields pages of results
    def __init__(self, config):
        # V2 API uses header-based key auth
        self.api_key = config['api_key']

    def make_request(self, url, params):
        # Single request helper.
        # Ensures page_size is set and auth headers are included.
        LOGGER.info('Making request to %s with query parameters %s', url, params)
        params['page_size'] = PAGE_SIZE

        headers = {
            'Content-Type': 'application/json',
            'api-key': self.api_key,
            'SS-API-KEY': self.api_key
        }

        response = requests.get(url, params=params, headers=headers)
        return response

    def paginate(self, endpoint, params):
        # Generator that walks through all pages for a given endpoint.
        # Yields a list of items per page and handles:
        # - JSON parsing edge cases (HTML error pages)
        # - Basic rate limiting (waits on remaining/reset headers when present)
        # - Common HTTP errors (401/403/429)
        url = BASE_URL + endpoint
        tried_legacy = False
        legacy_param_renamed = False
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
                LOGGER.error('Authentication failed (401). Header-based API key was rejected. Verify the key.')
                response.raise_for_status()
            elif status_code == 403:
                LOGGER.error('Forbidden. Please check your API permissions.')
                response.raise_for_status()
            elif status_code == 429:
                time.sleep(60)
                LOGGER.info("Waiting for 60 seconds due to 429 without warning")
            else:
                # Fallback logic specifically for orders endpoint: if v2 path 404s, try legacy base
                if endpoint == 'orders' and status_code == 404 and not tried_legacy:
                    # Detailed diagnostics per manager suggestion
                    LOGGER.warning('orders endpoint returned 404 on primary v2 path: %s', url)
                    LOGGER.warning('Response headers for 404: %s', {k: v for k, v in headers.items()})
                    LOGGER.warning('Response body (truncated 500 chars): %s', response.text[:500])
                    LOGGER.warning('Attempting alternate endpoint discovery for orders...')

                    # Build param variant sets
                    original_params = params.copy()
                    param_variants = []
                    # Variant 1: current style (created_at_start/end + page_size)
                    param_variants.append(original_params.copy())
                    # Variant 2: ShipStation classic createDateStart/createDateEnd + pageSize
                    v2 = original_params.copy()
                    start = v2.pop('created_at_start', None)
                    end = v2.pop('created_at_end', None)
                    if start:
                        v2['createDateStart'] = start
                    if end:
                        v2['createDateEnd'] = end
                    if 'page_size' in v2:
                        v2['pageSize'] = v2.pop('page_size')
                    param_variants.append(v2)
                    # Variant 3: orderDateStart/orderDateEnd (some APIs support orderDate filters)
                    v3 = v2.copy()
                    if 'createDateStart' in v3:
                        v3['orderDateStart'] = v3['createDateStart']
                    if 'createDateEnd' in v3:
                        v3['orderDateEnd'] = v3['createDateEnd']
                    param_variants.append(v3)

                    # Candidate base URLs (ShipStation classic + ShipEngine) in order of likelihood
                    candidate_bases = []
                    candidate_bases.extend(LEGACY_BASE_URLS)
                    candidate_bases.extend(ALT_ENGINE_BASE_URLS)

                    for base in candidate_bases:
                        for pset in param_variants:
                            candidate_url = base + endpoint if not base.rstrip('/').endswith('/v1') else base + endpoint  # keep uniform
                            LOGGER.info('Trying candidate orders URL %s with params %s', candidate_url, pset)
                            try:
                                test_resp = self.make_request(candidate_url, pset.copy())
                            except Exception as e:
                                LOGGER.error('Candidate request exception %s: %s', candidate_url, e)
                                continue
                            if test_resp.status_code == 200:
                                LOGGER.info('Discovered working orders endpoint: %s', candidate_url)
                                response = test_resp
                                headers = response.headers
                                status_code = response.status_code
                                url = candidate_url
                                tried_legacy = True
                                params = pset  # adopt working params for pagination loop
                                try:
                                    response.json()
                                except Exception as e:
                                    LOGGER.error('JSON parse failed on discovered orders endpoint: %s', e)
                                    raise
                                # Continue with normal success handling
                                break
                            else:
                                LOGGER.warning('Candidate %s returned status %s', candidate_url, test_resp.status_code)
                                # If unauthorized, we may need secret-based auth; log and keep scanning
                                if test_resp.status_code in (401, 403):
                                    LOGGER.warning('Auth issue on %s; may require classic key+secret Basic auth.', candidate_url)
                        if tried_legacy:
                            break

                    if not tried_legacy:
                        LOGGER.error('All alternate orders endpoint attempts failed (still 404).')
                    else:
                        # Restart loop body with newly discovered endpoint
                        continue
                else:
                    LOGGER.error('Request failed with status %s', status_code)
                LOGGER.error('Response content: %s', response.text[:1000])
                response.raise_for_status()