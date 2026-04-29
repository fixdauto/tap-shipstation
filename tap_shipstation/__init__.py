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

# API v2: headers-based auth by default
REQUIRED_CONFIG_KEYS = ['api_key']
LOGGER = singer.get_logger()


def get_abs_path(path):
    # Return absolute path rooted at this module's directory.
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    # Read v2-supported schema files and return a dict keyed by stream name.
    # Explicitly filter out deprecated/legacy streams like 'orders'.
    allowed_streams = {'shipments', 'fulfillments', 'ap_shipment_tracking'}
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        file_raw = filename.replace('.json', '')
        if file_raw not in allowed_streams:
            continue
        path = get_abs_path('schemas') + '/' + filename
        with open(path) as file:
            schemas[file_raw] = jsonref.load(file)

    return schemas


def discover():
    # Build a Singer catalog from local schemas and default metadata.
    raw_schemas = load_schemas()

    streams = []

    keys = {
        'shipments': ['shipment_id'],
        'fulfillments': ['fulfillment_id'],
        'ap_shipment_tracking': ['shipment_id']
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


def _get_stream_from_catalog(catalog, stream_id):
    """Helper to get a stream by id from catalog."""
    for stream in catalog.streams:
        if stream.tap_stream_id == stream_id:
            return stream
    return None


def _build_tracking_record(shipment, tracking_data, fetched_at):
    """Build a tracking record combining shipment and tracking API data."""
    ship_to = shipment.get('ship_to') or {}

    record = {
        'shipment_id': shipment.get('shipment_id'),
        'shipment_number': shipment.get('shipment_number'),
        'label_id': tracking_data.get('label_id'),
        'tracking_number': tracking_data.get('tracking_number'),
        'carrier_code': tracking_data.get('carrier_code'),
        'status_code': tracking_data.get('status_code'),
        'status_description': tracking_data.get('status_description'),
        'carrier_status_code': tracking_data.get('carrier_status_code'),
        'carrier_status_description': tracking_data.get('carrier_status_description'),
        'shipped_date': tracking_data.get('shipped_date'),
        'estimated_delivery_date': tracking_data.get('estimated_delivery_date'),
        'actual_delivery_date': tracking_data.get('actual_delivery_date'),
        'exception_description': tracking_data.get('exception_description'),
        'events': tracking_data.get('events', []),
        'ship_to_name': ship_to.get('name'),
        'ship_to_company_name': ship_to.get('company_name'),
        'ship_to_email': ship_to.get('email'),
        'ship_to_state_province': ship_to.get('state_province'),
        'fetched_at': fetched_at
    }
    return record


def _get_label_id_from_shipment(shipment):
    """Extract label_id from shipment packages if available."""
    packages = shipment.get('packages') or []
    for pkg in packages:
        label_id = pkg.get('label_id')
        if label_id:
            return label_id
    return None


def _get_tracking_info_from_shipment(shipment):
    """Extract carrier_code and tracking_number from shipment packages."""
    packages = shipment.get('packages') or []
    for pkg in packages:
        tracking = pkg.get('tracking_number')
        if tracking:
            carrier = shipment.get('carrier_id') or shipment.get('carrier_code')
            return carrier, tracking
    return None, None


def sync(config, state, catalog):
    # Core extraction loop:
    # - Determine selected streams
    # - For each selected stream, compute start/end window
    # - Iterate day-by-day, paginate ShipStation API, transform, write records
    # - Persist bookmark at the end of each day
    # - If ap_shipment_tracking is selected, fetch tracking for APPro shipments
    if isinstance(catalog, dict):
        catalog = Catalog.from_dict(catalog)
    selected_stream_ids = get_selected_streams(catalog)

    tracking_stream_id = 'ap_shipment_tracking'
    tracking_selected = tracking_stream_id in selected_stream_ids
    tracking_schema_written = False
    tracking_stream = _get_stream_from_catalog(catalog, tracking_stream_id)

    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema

        if stream_id == tracking_stream_id:
            continue

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
            if stream_id == 'fulfillments':
                start_at = pendulum.now('America/Los_Angeles').subtract(days=7)
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

            # Shipments and Fulfillments: use created_at filters (v2 behavior)
            if stream_id == 'shipments':
                params = {
                    'created_at_start': start_at.strftime('%Y-%m-%d'),
                    'created_at_end': end_at.strftime('%Y-%m-%d'),
                    'page': 1
                }
            elif stream_id == 'fulfillments':
                # Independent v2 fulfillments window, same pattern as shipments
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
                if stream_id == 'fulfillments':
                    pages = client.paginate_fulfillments_v2(params)
                else:
                    pages = client.paginate(stream_id, params)
                debug_sample = os.getenv('SHIPSTATION_DEBUG_SAMPLE', 'false').lower() == 'true'
                bypass_transform = os.getenv('SHIPSTATION_BYPASS_TRANSFORM', 'false').lower() == 'true'
                first_logged = False
                first_transformed_logged = False
                for page in pages:
                    for record in page:
                        # For fulfillments, enforce a strict client-side window filter to avoid history dumps
                        if stream_id == 'fulfillments':
                            ts_str = record.get('created_at') or record.get('ship_date') or record.get('delivered_at')
                            try:
                                ts = pendulum.parse(ts_str) if ts_str else None
                            except Exception:
                                ts = None
                            if (ts is None) or not (start_at <= ts < end_at):
                                continue
                        if stream_id in ('shipments', 'fulfillments') and debug_sample and not first_logged:
                            try:
                                LOGGER.info('Sample %s record keys (first item): %s', stream_id, sorted(list(record.keys())))
                            except Exception:
                                LOGGER.info('Sample %s record available but failed to log keys.', stream_id)
                            first_logged = True

                        if bypass_transform:
                            singer.write_record(stream_id, record)
                        else:
                            transformed = singer.transform(record, stream_schema.to_dict())
                            if stream_id in ('shipments', 'fulfillments') and debug_sample and not first_transformed_logged:
                                try:
                                    LOGGER.info('Sample transformed %s record keys (first item): %s', stream_id, sorted(list(transformed.keys())))
                                except Exception:
                                    LOGGER.info('Transformed sample available but failed to log keys for %s.', stream_id)
                                first_transformed_logged = True
                            singer.write_record(stream_id, transformed)

                        # Tracking: for APPro shipments, fetch and emit tracking data
                        if (stream_id == 'shipments' and tracking_selected and
                                tracking_stream is not None):
                            shipment_number = record.get('shipment_number') or ''
                            if shipment_number.startswith('AP'):
                                if not tracking_schema_written:
                                    singer.write_schema(
                                        tracking_stream_id,
                                        tracking_stream.schema.to_dict(),
                                        tracking_stream.key_properties)
                                    tracking_schema_written = True

                                fetched_at = pendulum.now('UTC').to_iso8601_string()
                                tracking_data = None

                                label_id = _get_label_id_from_shipment(record)
                                if label_id:
                                    tracking_data = client.get_tracking_by_label(label_id)

                                if not tracking_data:
                                    carrier, tracking_num = _get_tracking_info_from_shipment(record)
                                    if carrier and tracking_num:
                                        tracking_data = client.get_tracking_by_number(carrier, tracking_num)

                                if tracking_data:
                                    tracking_record = _build_tracking_record(record, tracking_data, fetched_at)
                                    singer.write_record(tracking_stream_id, tracking_record)
                                else:
                                    LOGGER.info('No tracking data available for shipment %s', shipment_number)

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