import re
import singer
import dateutil

LOGGER = singer.get_logger()

date_regex = re.compile(r"^\d{4}-[01]?\d-[0-3]?\d$")
datetime_regex = re.compile(r"^\d{4}-[01]\d-[0-3]\dT\d{2}\:\d{2}\:\d{2}Z$")

def infer(datum):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        int(datum)
        return 'integer'
    except (ValueError, TypeError):
        pass

    try:
        #numbers are NOT floats, they are DECIMALS
        float(datum)
        return 'number'
    except (ValueError, TypeError):
        pass

    if date_regex.search(datum) or datetime_regex.search(datum):
        return 'date-time'

    return 'string'


def count_sample(sample, counts):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        datatype = infer(value)

        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts


def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if counts.get('date-time', 0) > 0:
        return 'date-time'

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return


def generate_schema(samples):
    counts = {}
    for sample in samples:
        # {'name' : { 'string' : 45}}
        counts = count_sample(sample, counts)

    for key, value in counts.items():
        datatype = pick_datatype(value)

        if datatype == 'date-time':
            counts[key] = {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date-time'},
                    {'type': ['null', 'string']}
                ]
            }
        else:
            types = ['null', datatype]
            if datatype != 'string':
                types.append('string')
            counts[key] = {
                'type': types,
            }

    return counts
