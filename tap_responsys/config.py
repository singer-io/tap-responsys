from voluptuous import Schema, Required, Optional, ALLOW_EXTRA

def coercible_int(val):
    """ Validates by attempting to coerce the value to int, throws ValueError if not possible. """
    return int(val)

CONFIG_CONTRACT = Schema({
    Required('start_date'): str,
    Required('host'): str,
    Required('username'): str,
    Required('path'): str,
    Required('private_key_file'): str,
    Optional('port'): coercible_int
}, extra=ALLOW_EXTRA)
