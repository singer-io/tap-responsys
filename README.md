# tap-responsys

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap pulls data from CSV files exported to an SFTP server through Responsys' Connect export jobs. Features of the extraction are:

- Automatic Stream Name discovery with the pattern `[optional_date_prefix]stream_name[optional_date_suffix].[csv|txt]`
- Automatic Schema discovery by sampling 1000 records from the first 5 files found per stream
- Bookmarking on the file's `last_modified` timestamp, and only requesting new files greater than that value on future runs with `--state` specified

## Requirements of Exports

In order to handle the wealth of options available for exports in Responsys Connect, some things are assumed about the files being discovered and extracted using this tap:

- Ready files are required to ensure all data has been written before reading, the extension `ready` should be used.
- UTF-8 is required
- CSV exports must be comma-delimited and quoted with a double-quote character (`"`)
- Column headers are strictly ***required***
- Encryption and compression are not supported at this time

## Quick start

1. Install

    ```bash
    > pip install tap-responsys
    ```

2. Upload your public key to the SFTP server used by Responsys

    The tap will need to use an SSH public/private keypair to connect to the SFTP server. In order for this to work, the server needs to trust the public key.
    
    **If using the hosted Responsys SFTP Server**: You can work with Responsys to get the public key uploaded and trusted on the server. 
    
    **If using a custom SFTP server**: Consult with the administrator of that server to upload and trust the SSH public key file.


3. Create the config file

    Create a JSON file called `config.json` containing the local path to the private key file, and server information.

    ```json
    {
      "private_key_file": "~/.ssh/responsys_rsa",
      "username": "test_scp",
      "start_date": "2017-09-21T00:00:00Z",
      "path": "exports",
      "host": "files.responsys.net",
      "port": "22"
    }
    ```

5. Run the application

    **Discovery mode**

    ```bash
    tap-responsys --config config.json --discover > catalog.json
    ```

    **Sync Mode**

    You only need to add `"selected": true` metadata to the stream level in the catalog, since fields are selected automatically. Once that is done, you can run sync mode using this command, with optional state from a previous run:

    ```bash
    tap-responsys --config config.json --catalog catalog.json [--state state.json]
    ```

---

Copyright &copy; 2018 Stitch
