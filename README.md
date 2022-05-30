Experiments with rust-libp2p

USAGE:

./domo-libp2p <sqlite_file_path>

Commands:

HASH: shows the current hash of the local cache
PRINT: prints the local cache content
PEERS: prints the list of discovered peers
PUB <topic_name> <topic_uuid> <value>: publishes a message and stores it in the local cache

Example:

PUB Domo::Light first-light on

