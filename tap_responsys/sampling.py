from io import RawIOBase
from singer_encodings import csv
import singer
from tap_responsys import sftp, conversion

LOGGER = singer.get_logger()

SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"

def get_sampled_schema_for_table(conn, prefix, table_name):
    LOGGER.info('Sampling records to determine table schema "%s".', table_name)

    files = conn.get_files_for_table(prefix, table_name)

    if not files:
        return {}

    samples = sample_files(conn, table_name, files)

    metadata_schema = {
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        csv.SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}},
    }

    data_schema = conversion.generate_schema(samples)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }

def sample_file(conn, table_name, filepath, sample_rate, max_records):
    LOGGER.info('Sampling %s (%s records, every %sth record).', filepath, max_records, sample_rate)

    samples = []

    file_handle = conn.get_file_handle(filepath)

    class RawStream(RawIOBase):
        def __init__(self, sftp_stream):
            self._sftp_stream = sftp_stream
            self.read = sftp_stream.read

    raw_stream = RawStream(file_handle)
    iterator = csv.get_row_iterator(raw_stream)

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            if row.get(csv.SDC_EXTRA_COLUMN):
                row.pop(csv.SDC_EXTRA_COLUMN)
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    LOGGER.info('Sampled %s records.', len(samples))

    return samples

# pylint: disable=too-many-arguments
def sample_files(conn, table_name, files,
                 sample_rate=5, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for f in files:
        to_return += sample_file(conn, table_name, f,
                                 sample_rate, max_records)

        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return

def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return