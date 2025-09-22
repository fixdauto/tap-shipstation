import os
import json
import jsonref
from datetime import datetime
from datetime import timedelta
import pendulum
import singer
from singer import utils, metadata
from singer.catalog import Catalog
from .client import ShipStationClient
from .client import prepare_datetime

# Auth options:
# - Header-based (default): send API key in headers ("api-key" and "SS-API-KEY").
# - Basic Auth (optional): if "api_secret" is provided or "auth_mode" is set to "basic",
#   use Basic Auth with (api_key, api_secret).
# Minimal required keys:
REQUIRED_CONFIG_KEYS = ['api_key', 'default_start_datetime']
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = jsonref.load(file)

    return schemas

def discover():
    raw_schemas = load_schemas()

    streams = []

    keys = {
        'orders' : ['orderId'],
        'shipments' : ['shipmentId']
    }

    for schema_name, schema in raw_schemas.items():
        top_level_metadata = {
            'selected': True,
            'selected-by-default': True,
            'inclusion': 'available',
            'table-key-properties': keys[schema_name]}

        metadata_entry = singer.metadata.new()
        for key, value in top_level_metadata.items():
            metadata_entry = singer.metadata.write(
                compiled_metadata=metadata_entry,
                breadcrumb=(),
                k=key,
                val=value)

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'key_properties': keys[schema_name],
            'metadata' : singer.metadata.to_list(metadata_entry)
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams

def sync(config, state, catalog):
    # Accept either a Singer Catalog or a plain dict from discover()
    if isinstance(catalog, dict):
        catalog = Catalog.from_dict(catalog)
    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        if stream_id in selected_stream_ids:
            LOGGER.info("Beginning sync of stream '%s'.", stream_id)
            singer.write_schema(
                stream_id,
                stream_schema.to_dict(),
                stream.key_properties)

            client = ShipStationClient(config)
            bookmark = singer.get_bookmark(
                state=state,
                tap_stream_id=stream_id,
                key='modifyDate')

            if bookmark:
                start_at = pendulum.parse(bookmark, tz='America/Los_Angeles')
            else:
                start_at = pendulum.parse(config['default_start_datetime'], tz='America/Los_Angeles')

            stream_end_at = pendulum.now('America/Los_Angeles')
            # Testing Here: If SHIPSTATION_TEST_ONE_DAY=true, limit to a single day window for quick validation
            if os.getenv('SHIPSTATION_TEST_ONE_DAY', 'false').lower() == 'true':
                test_end = start_at + timedelta(days=1)
                if test_end < stream_end_at:
                    stream_end_at = test_end
                LOGGER.info('SHIPSTATION_TEST_ONE_DAY enabled; limiting stream_end_at to %s', stream_end_at)

            # V2 API: Simplified - no need for multiple date query types

            end_at = start_at
            while end_at < stream_end_at:
                #Increment queries by 1 day, limit to stream end datetime
                end_at += timedelta(days=1)
                if end_at > stream_end_at:
                    end_at = stream_end_at
                # V2 API: Focus on shipments only with created_at parameters 
                # Use DATE-ONLY strings (YYYY-MM-DD) and a 1-day window. Example:
                #   GET https://api.shipstation.com/v2/shipments?created_at_start=2025-09-01&page=1&page_size=100
                if stream_id == 'shipments':
                    params = {
                        'created_at_start': start_at.strftime('%Y-%m-%d'),
                        'created_at_end': end_at.strftime('%Y-%m-%d'),
                        'page': 1
                    }
                else:
                    # Skip non-shipment streams for v2 API focus
                    LOGGER.info('Skipping stream %s - focusing on shipments only for v2', stream_id)
                    continue
                
                # CUSTOM FIX: Added try/catch around pagination to handle errors gracefully
                # ORIGINAL CODE: No error handling - would crash the entire pipeline on any error
                try:
                    pages = client.paginate(stream_id, params)
                    for page in pages:
                        for record in page:
                            transformed = singer.transform(record, stream_schema.to_dict())
                            singer.write_record(stream_id, transformed)
                except Exception as e:
                    # CUSTOM FIX: Log error details and continue instead of crashing
                    LOGGER.error('Error processing stream %s with params %s: %s', stream_id, params, str(e))
                    # Continue to next query instead of crashing
                    continue

                #Write state at end of daily loop for stream
                state = singer.write_bookmark(
                    state=state,
                    tap_stream_id=stream_id,
                    key='modifyDate',
                    val=end_at.strftime("%Y-%m-%d %H:%M:%S"))
                singer.write_state(state)
                start_at = end_at              

            LOGGER.info("Finished syncing stream '%s'.", stream_id)

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            # Convert discover dict into Singer Catalog for compatibility with sync()
            catalog = Catalog.from_dict(discover())

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()