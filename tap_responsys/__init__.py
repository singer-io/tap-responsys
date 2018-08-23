import json
import sys
import singer

from singer import metadata
from tap_responsys.discover import discover_streams
#from tap_s3_csv.s3 import get_bucket_config
#from tap_s3_csv.sync import sync_stream
from tap_responsys.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date"]

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)

if __name__ == '__main__':
    main()

    # from tap_responsys import sftp
    # import csv
    # conn = sftp.open_connection(config)
    # with conn.sftp.open("exports/test_file.csv", "r") as f:
    #     reader = csv.DictReader(f)
    #     for row in reader:
    #         print(row)
