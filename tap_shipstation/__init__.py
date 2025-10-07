"""
Implements a Singer tap for the ShipStation v2 API.

The tap operates in two main modes: Discovery and Sync.

Discovery Mode:
- The `discover()` function builds a Singer Catalog by dynamically loading JSON schemas
    from the `./schemas` directory.
- It enriches the catalog with default metadata, including `selected-by-default`
    and `table-key-properties` for streams like 'shipments' and 'orders'.

Sync Mode:
- The `sync()` function handles the core data extraction process.
- It performs incremental extraction using a bookmark based on the `created_at` field.
- On the first run without a state file, it defaults to syncing data from the last 30 days.
- To manage API rate limits and ensure predictable request sizes, data is fetched in
    daily windows.
- For each selected stream, it iterates day-by-day from the start bookmark to the
    present, paginating through API results.
- Records are transformed against their JSON schema before being written to stdout
    as Singer messages.
- The state (bookmark) is persisted after each successfully synced day.

The main entry point, `main()`, parses command-line arguments to run either the
discovery or sync process.
"""
import os
import json
import jsonref
from datetime import timedelta
import pendulum
import singer
from singer import utils, metadata
from singer.catalog import Catalog
from .client import ShipStationClient

# API v2: header-based auth by default
REQUIRED_CONFIG_KEYS = ['api_key']
LOGGER = singer.get_logger()


def get_abs_path(path):
    # Return absolute path rooted at this module's directory.
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    # Read all schema files and return a dict keyed by stream name.
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = jsonref.load(file)

    return schemas


def discover():
    # Build a Singer catalog from local schemas and default metadata.
    raw_schemas = load_schemas()

    streams = []

    keys = {
        'shipments': ['shipment_id'],
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
    # Return list of stream ids where top-level metadata 'selected' is True.
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(config, state, catalog):
    # Core extraction loop:
    # - Determine selected streams
    # - For each selected stream, compute start/end window
    # - Iterate day-by-day, paginate ShipStation API, transform, write records
    # - Persist bookmark at the end of each day
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
            LOGGER.info("No bookmark found. Syncing last 30 days.")
            start_at = pendulum.now('America/Los_Angeles').subtract(days=30)

        stream_end_at = pendulum.now('America/Los_Angeles')
        if os.getenv('SHIPSTATION_TEST_ONE_DAY', 'false').lower() == 'true':
            test_end = start_at.add(days=1)
            if test_end < stream_end_at:
                stream_end_at = test_end
            LOGGER.info('SHIPSTATION_TEST_ONE_DAY enabled; limiting stream_end_at to %s', stream_end_at)

        end_at = start_at
        while end_at < stream_end_at:
            end_at = min(end_at.add(days=1), stream_end_at)

            # ShipStation v2 shipments endpoint supports created_at_* filters.
            # For orders, documentation is inconsistent; we'll first attempt created_at_*
            # (some tenants expose a unified timestamp) and fall back to order_date_*.
            if stream_id in ('shipments', 'orders'):
                params = {
                    'created_at_start': start_at.strftime('%Y-%m-%d'),
                    'created_at_end': end_at.strftime('%Y-%m-%d'),
                    'page': 1
                }
            else:
                LOGGER.info('Skipping unsupported stream %s', stream_id)
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
                        if stream_id in ('shipments', 'orders') and debug_sample and not first_logged:
                            try:
                                LOGGER.info('Sample %s record keys (first item): %s', stream_id, sorted(list(record.keys())))
                            except Exception:
                                LOGGER.info('Sample %s record available but failed to log keys.', stream_id)
                            first_logged = True

                        if stream_id == 'orders':
                            # Normalize timestamp field so bookmark logic (created_at) remains consistent.
                            # Prefer createDate, then orderDate, then modifyDate.
                            created_like = record.get('createDate') or record.get('orderDate') or record.get('modifyDate')
                            if created_like and 'created_at' not in record:
                                record['created_at'] = created_like

                        if bypass_transform:
                            singer.write_record(stream_id, record)
                        else:
                            transformed = singer.transform(record, stream_schema.to_dict())
                            if stream_id in ('shipments', 'orders') and debug_sample and not first_transformed_logged:
                                try:
                                    LOGGER.info('Sample transformed %s record keys (first item): %s', stream_id, sorted(list(transformed.keys())))
                                except Exception:
                                    LOGGER.info('Transformed sample available but failed to log keys for %s.', stream_id)
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