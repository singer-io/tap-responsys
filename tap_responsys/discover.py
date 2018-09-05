from singer import metadata
from tap_responsys import sftp
from tap_responsys import sampling

def discover_streams(config):
    streams = []

    conn = sftp.connection(config)
    exported_tables = conn.get_exported_tables(config["path"])

    for exported_table in exported_tables:
        schema = sampling.get_sampled_schema_for_table(conn, config["path"], exported_table)
        streams.append({'stream': exported_table, 'tap_stream_id': exported_table, 'schema': schema, 'metadata': load_metadata(schema)})
    return streams


def load_metadata(schema):
    mdata = metadata.new()

    # Make all fields automatic
    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    return metadata.to_list(mdata)
