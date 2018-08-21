from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema({
    Required('host'): str,
    Required('username'): str,
    Optional('password'): str,
    Optional('private_key_file'): str,
    Optional('port'): str
})
