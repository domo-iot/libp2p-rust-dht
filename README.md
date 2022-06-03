Experiments with rust-libp2p

USAGE:

**./domo-libp2p <sqlite_file_path>**

Commands:

**HASH**: shows the current hash of the local cache

**PRINT**: prints the local cache content

**PEERS**: prints the list of discovered peers

**PUT <topic_name> <topic_uuid> <value>**: publishes a message and stores it in the caches

**DEL <topic_name> <topic_uuid>**: deletes (topic_name, topic_uuid) 

**PUB <value>**: publishes a volatile message

Examples:

- PUT Domo::Light first-light on
- DELETE Domo::Light first-light
- PUB ciao

