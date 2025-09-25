import os
import json
import jsonref
from datetime import timedelta
import pendulum
import singer
from singer import utils, metadata
from singer.catalog import Catalog
from .client import ShipStationClient

# API v2: header-based auth by default; basic auth available if api_secret provided
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
        'shipments': ['shipment_id'],
        # Keep placeholder for orders in case it's added later; currently we focus on shipments only
        'orders': ['orderId']
    }

    for schema_name, schema in raw_schemas.items():
        top_level_metadata = {
            'selected': True,
            'selected-by-default': True,
            'inclusion': 'available',
            'table-key-properties': keys.get(schema_name, [])
        }

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
            'key_properties': keys.get(schema_name, []),
            'metadata': singer.metadata.to_list(metadata_entry)
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(config, state, catalog):
    if isinstance(catalog, dict):
        catalog = Catalog.from_dict(catalog)
    selected_stream_ids = get_selected_streams(catalog)

    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        if stream_id not in selected_stream_ids:
            continue

        LOGGER.info("Beginning sync of stream '%s'.", stream_id)
        singer.write_schema(
            stream_id,
            stream_schema.to_dict(),
            stream.key_properties)

        client = ShipStationClient(config)
        # Bookmark alignment: we filter by created_at_* params, so store bookmark under 'created_at'.
        # Backward compatibility: fall back to legacy 'modifyDate' bookmark if present.
        bookmark = singer.get_bookmark(
            state=state,
            tap_stream_id=stream_id,
            key='created_at') or singer.get_bookmark(
                state=state,
                tap_stream_id=stream_id,
                key='modifyDate')

        if bookmark:
            start_at = pendulum.parse(bookmark, tz='America/Los_Angeles')
        else:
            start_at = pendulum.parse(config['default_start_datetime'], tz='America/Los_Angeles')

        stream_end_at = pendulum.now('America/Los_Angeles')
        if os.getenv('SHIPSTATION_TEST_ONE_DAY', 'false').lower() == 'true':
            test_end = start_at.add(days=1)
            if test_end < stream_end_at:
                stream_end_at = test_end
            LOGGER.info('SHIPSTATION_TEST_ONE_DAY enabled; limiting stream_end_at to %s', stream_end_at)

        end_at = start_at
        while end_at < stream_end_at:
            end_at = min(end_at.add(days=1), stream_end_at)

            if stream_id == 'shipments':
                params = {
                    'created_at_start': start_at.strftime('%Y-%m-%d'),
                    'created_at_end': end_at.strftime('%Y-%m-%d'),
                    'page': 1
                }
            else:
                LOGGER.info('Skipping stream %s - focusing on shipments only for v2', stream_id)
                # Still advance bookmark to avoid reruns if orders exists in catalog
                state = singer.write_bookmark(
                    state=state,
                    tap_stream_id=stream_id,
                    key='created_at',
                    val=end_at.strftime("%Y-%m-%d %H:%M:%S"))
                singer.write_state(state)
                start_at = end_at
                continue

            try:
                pages = client.paginate(stream_id, params)
                debug_sample = os.getenv('SHIPSTATION_DEBUG_SAMPLE', 'false').lower() == 'true'
                bypass_transform = os.getenv('SHIPSTATION_BYPASS_TRANSFORM', 'false').lower() == 'true'
                first_logged = False
                first_transformed_logged = False
                for page in pages:
                    for record in page:
                        if stream_id == 'shipments' and debug_sample and not first_logged:
                            try:
                                LOGGER.info('Sample shipment record keys (first item): %s', sorted(list(record.keys())))
                            except Exception:
                                LOGGER.info('Sample shipment record available but failed to log keys.')
                            first_logged = True

                        if bypass_transform:
                            singer.write_record(stream_id, record)
                        else:
                            transformed = singer.transform(record, stream_schema.to_dict())
                            if stream_id == 'shipments' and debug_sample and not first_transformed_logged:
                                try:
                                    LOGGER.info('Sample transformed shipment record keys (first item): %s', sorted(list(transformed.keys())))
                                except Exception:
                                    LOGGER.info('Transformed sample available but failed to log keys.')
                                first_transformed_logged = True
                            singer.write_record(stream_id, transformed)
            except Exception as e:
                LOGGER.error('Error processing stream %s with params %s: %s', stream_id, params, str(e))
                continue

            state = singer.write_bookmark(
                state=state,
                tap_stream_id=stream_id,
                key='created_at',
                val=end_at.strftime("%Y-%m-%d %H:%M:%S"))
            singer.write_state(state)
            start_at = end_at

        LOGGER.info("Finished syncing stream '%s'.", stream_id)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        discovery = discover()
        catalog_obj = Catalog.from_dict(discovery)
        print(json.dumps(catalog_obj.to_dict(), indent=2))
    else:
        catalog = args.catalog or Catalog.from_dict(discover())
        sync(args.config, args.state, catalog)
