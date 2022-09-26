The SIFIS-HOME DHT is a component that offers a completely distributed publish/subscribe mechanism through which SIFIS-HOME applications can exchange messages. The SIFIS-HOME DHT allows to publish both persistent and volatile messages. Persistent messages are messages that need to be stored in a persistent way, so that they are available even after a node reboot operation. Volatile messages are instead messages that need to be delivered to all the available applications but that do not need to be persisted on disk. The SIFIS-HOME DHT has a built-in mechanism to solve possible data conflicts that can arise when a network partition occurs. In detail, every time a message is published on the DHT, the DHT also stores its publication timestamp. Then, the publication timestamp is used to assure that only the most recently published messages will be stored and made available to the applications.

USAGE:

sifis-dht <SQLITE_FILE> <IS_PERSISTENT_CACHE> <SHARED_KEY> <HTTP_PORT> <LOOPBACK_ONLY>

where:

SQLITE_FILE: absolute path of the sqlite file where persistent messages published on the DHT are stored.

IS_PERSISTENT_CACHE: if set to true indicates that domo-libp2p is authorized to write messages to the provided sqlite file. If set to false, the SQLITE_FILE content will only be used to initialize the cache.

SHARED_KEY: 32 bytes long shared key in hex format (command "openssl rand -hex 32" can be used to generate a random key)

HTTP_PORT: port to be used for the HTTP interface

LOOPBACK_ONLY: if set to true, only the loopback interface will be used, meaning that only other local instances of sifis-dht are discovered. If set to false, all the available network interfaces of the device will be used. Hence, two sifis-dht instances running on the same local network should discover each other.
