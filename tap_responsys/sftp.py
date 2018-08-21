import os
import paramiko

class SFTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = port or 22
        self.private_key_file = private_key_file

    def __enter__(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.use_compression(True)
        self.key = None
        if self.private_key_file:
            key_path = os.path.expanduser(self.private_key_file)
            self.key = paramiko.RSAKey.from_private_key_file(key_path)
        self.creds = {'username': self.username, 'password': self.password,
                      'hostkey': None, 'pkey': self.key}
        self.transport.connect(**self.creds)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        return self.sftp

    def __exit__(self, *args):
        self.sftp.close()
        self.transport.close()


def open_connection(config):
    return SFTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          private_key_file=config.get('private_key_file'),
                          port=config.get('port'))
