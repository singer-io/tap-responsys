import io
import os
import paramiko
import pytz
import re
import singer
import stat
import time
from io import RawIOBase
from datetime import datetime
from paramiko.ssh_exception import AuthenticationException

LOGGER = singer.get_logger()

class SFTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = port or 22
        self.private_key_file = private_key_file
        self.__active_connection = False

    def __ensure_connection(self):
        if not self.__active_connection:
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.use_compression(True)
            self.key = None
            key_path = os.path.expanduser(self.private_key_file)
            self.key = paramiko.RSAKey.from_private_key_file(key_path)

            try:
                self.creds = {'username': self.username, 'password': None,
                              'hostkey': None, 'pkey': self.key}
                self.transport.connect(**self.creds)
            except AuthenticationException as ex:
                raise Exception("Message from SFTP server: {} - Please ensure that the server is configured to accept the public key for this integration.".format(ex)) from ex
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.__active_connection = True

    @property
    def sftp(self):
        self.__ensure_connection()
        return self.__sftp

    @sftp.setter
    def sftp(self, sftp):
        self.__sftp = sftp

    def __del__(self):
        """ Clean up the socket when this class gets garbage collected. """
        self.close()

    def close(self):
        if self.__active_connection:
            self.sftp.close()
            self.transport.close()
            self.__active_connection = False

    def get_files_by_prefix(self, prefix):
        """
        Accesses the underlying file system and gets all files that match "prefix", in this case, a directory path.

        Returns a list of filepaths from the root.
        """
        files = []
        try:
            result = self.sftp.listdir_attr(prefix)
        except FileNotFoundError as e:
            raise Exception("Directory '{}' does not exist".format(prefix)) from e

        is_file = lambda a: stat.S_ISREG(a.st_mode)
        for file_attr in result:
            # NB: This only looks at the immediate level
            if is_file(file_attr):
                # NB: SFTP specifies path characters to be '/'
                #     https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
                files.append({"filepath": prefix + '/' + file_attr.filename,
                              "last_modified": datetime.utcfromtimestamp(file_attr.st_mtime).replace(tzinfo=pytz.UTC)})

        return files

    def get_exported_tables(self, prefix):
        files = self.get_files_by_prefix(prefix)

        if files:
            LOGGER.info("Found %s files.", len(files))
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s".', prefix)

        filenames = [o["filepath"].split('/')[-1] for o in files]
        csv_pattern = '(?:\d{8}_\d{6})?(.+)\.csv$'
        LOGGER.info("Searching for exported tables using files that match pattern: %s", csv_pattern)
        csv_matcher = re.compile(csv_pattern) # Match YYYYMMDD_HH24MISStable_name.csv
        ready_matcher = re.compile('(?:\d{8}_\d{6})?(.+)\.ready$') # Match YYYYMMDD_HH24MISStable_name.ready

        csv_file_names = set([m.group(1) for m in
                              [csv_matcher.search(o) for o in filenames]
                              if m])
        names_with_ready_files = set([m.group(1) for m in
                                      [ready_matcher.search(o) for o in filenames]
                                      if m])

        return csv_file_names.intersection(names_with_ready_files)

    def get_files_for_table(self, prefix, table_name, modified_since=None):
        files = self.get_files_by_prefix(prefix)
        table_pattern = '(?:\d{8}_\d{6})?' + re.escape(table_name) + '\.csv$'
        LOGGER.info("Searching for files for table '%s', matching pattern: %s", table_name, table_pattern)
        matcher = re.compile(table_pattern) # Match YYYYMMDD_HH24MISStable_name.csv
        to_return = [f for f in files if matcher.search(f["filepath"])]
        if modified_since is not None:
            to_return = [f for f in to_return if f["last_modified"] >= modified_since]

        return to_return

    def get_file_handle(self, f):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file. """
        is_ready = True # False
        sleep_time = 1 # Start at 1 second, exponentially backoff
        filepath = f["filepath"]
        ready_file = re.sub('\.csv$', '.ready', filepath)

        while not is_ready:
            try:
                self.sftp.stat(ready_file)
                is_ready = True
            except IOError:
                LOGGER.info("No ready file found for %s, sleeping for %s seconds...", filepath, sleep_time)
                time.sleep(sleep_time)
                sleep_time *= 2

        # Read the whole file here and return a BytesIO object
        # NB: If CSV files become too large, read these to disk in a tmp dir and clean them up when finished
        return io.BytesIO(self.sftp.open(filepath, 'r').read())

    def get_files_matching_pattern(self, files, pattern):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns files matching that pattern. """
        matcher = re.compile(pattern)
        return [f for f in files if matcher.search(f["filepath"])]

def connection(config):
    return SFTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          private_key_file=config.get('private_key_file'),
                          port=config.get('port'))

class RawStream(RawIOBase):
    """ Helper class to pass into encodings, so that Paramiko matches the types expected by base Python IO. """
    def __init__(self, sftp_stream):
        self._sftp_stream = sftp_stream
        self.read = sftp_stream.read
