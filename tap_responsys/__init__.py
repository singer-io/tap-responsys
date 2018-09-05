import json
import sys
import singer

from singer import metadata
from tap_responsys.discover import discover_streams
from tap_responsys.sync import sync_stream
from tap_responsys.config import CONFIG_CONTRACT

LOGGER = singer.get_logger()

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("(No streams found) Streams will only be discovered if a '*.ready' file is present, to ensure consistency. Please select the option to write a file with extension 'ready' on export completion.")
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

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = [] # No primary keys for now
        singer.write_schema(stream_name, stream['schema'], [])

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args([])
    config = CONFIG_CONTRACT(args.config)

    if args.discover:
        do_discover(config)
    elif args.properties:
        do_sync(config, args.properties, args.state)

if __name__ == '__main__':
    main()
