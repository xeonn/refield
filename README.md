# refield

## CouchDB Field Renamer

`refield` is a command-line tool designed to rename fields in documents stored within a CouchDB database. It supports renaming nested fields using dot notation and offers a dry-run mode to preview changes before applying them.

## Features
- Rename fields in CouchDB documents
- Supports dot notation for nested fields
- Dry-run mode to preview changes without modifying the database
- Handles partitioned and non-partitioned tables

## Installation
Ensure you have Rust installed, then build the project:
```sh
cargo build --release
```

## Usage
Run the tool with the following command-line arguments:
```sh
./refield --url <COUCHDB_URL> --table <TABLE_NAME> --old <OLD_FIELD> --new <NEW_FIELD> [--dry-run]
```

### Arguments:
- `-u, --url`       : URL of the CouchDB database
- `-t, --table`     : Name of the table (or document type)
- `-o, --old`       : Old field name to be renamed (supports dot notation)
- `-n, --new`       : New field name to replace the old one
- `--dry-run`       : Enable dry-run mode to preview changes

### Example:
```sh
./refield --url http://localhost:5984 --table users --old profile.age --new profile.birth_year --dry-run
```

## License
This project is licensed under the MIT License.

## Author
Onn Khairuddin Ismail

