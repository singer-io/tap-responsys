import os
import paramiko
import re
import singer
import stat
import time
from datetime import datetime

LOGGER = singer.get_logger()

# Structure:
# 1. We have listing files (responsibility of the source accessor? or another file that does a generic algorithm)
# For this, we have two steps:
#     We have prefix match (path, etc.)
#     We have pattern match (Job_id_datetime_prefix, pattern search for s3, etc.)
# These steps get us the files to be included in the "table"
# 2. For Discovery, we have sampling based on the lists of files returned by the previous step
#     - I'm going to try to wrap this up in sampling.py
# 3. For sync, we simply return all files matched per the selection criteria, to be parsed by the singer-encodings implementation

# TODO: This is pending seeing the structure of the job and external ids
TABLE_NAME_REGEX = re.compile("\d+_\d+_(\w+)\.csv")

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
            if self.private_key_file:
                key_path = os.path.expanduser(self.private_key_file)
                self.key = paramiko.RSAKey.from_private_key_file(key_path)
            self.creds = {'username': self.username, 'password': self.password,
                          'hostkey': None, 'pkey': self.key}
            self.transport.connect(**self.creds)
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
        # TODO: Optional modified_since timestamp?
        files = []
        result = self.sftp.listdir_attr(prefix)

        is_file = lambda a: stat.S_ISREG(a.st_mode)
        for file_attr in result:
            # NB: This only looks at the immediate level
            if is_file(file_attr):
                # NB: SFTP specifies path characters to be '/'
                #     https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
                files.append({"filepath": prefix + '/' + file_attr.filename,
                              "last_modified": datetime.fromtimestamp(file_attr.st_mtime)})

        return files

    def get_exported_tables(self, prefix):
        files = self.get_files_by_prefix(prefix)

        if files:
            LOGGER.info("Found %s files.", len(files))
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s".', prefix)

        filenames = [o["filepath"].split('/')[-1] for o in files]
        csv_matcher = re.compile('(?:\d{8}_\d{6})?(.+)\.csv$') # Match YYYYMMDD_HH24MISStablename.csv
        exported_tables = [m.group(1) for m in [csv_matcher.search(o) for o in filenames] if m]

        return exported_tables

    def get_files_for_table(self, prefix, table_name):
        files = self.get_files_by_prefix(prefix)
        matcher = re.compile('(?:\d{8}_\d{6})?' + re.escape(table_name) + '\.csv$') # Match YYYYMMDD_HH24MISStablename.csv
        return [f for f in files if matcher.search(f["filepath"])]

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
                time.sleep(sleep_time)
                sleep_time *= 2

        return self.sftp.open(filepath, 'r')

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
