# sifis-dht

[![Actions Status][actions badge]][actions]
[![CodeCov][codecov badge]][codecov]
[![LICENSE][license badge]][license]

The SIFIS-HOME DHT is a component that offers a completely distributed publish/subscribe mechanism through which SIFIS-HOME applications can exchange messages.

The SIFIS-HOME DHT allows to publish both persistent and volatile messages. Persistent messages are messages that need to be stored in a persistent way, so that they are available even after a node reboot operation. Volatile messages are instead messages that need to be delivered to all the available applications but that do not need to be persisted on disk. The SIFIS-HOME DHT has a built-in mechanism to solve possible data conflicts that can arise when a network partition occurs. In detail, every time a message is published on the DHT, the DHT also stores its publication timestamp. Then, the publication timestamp is used to assure that only the most recently published messages will be stored and made available to the applications.

## Build

<em>cargo build --release</em>

You can find the sifis-dht executable in the <em>target/release</em> folder.

## Test

<em>cargo test</em>

## Usage

<em>sifis-dht <SQLITE_FILE> <PRIVATE_KEY_FILE> <IS_PERSISTENT_CACHE> <SHARED_KEY> <HTTP_PORT> <LOOPBACK_ONLY></em>

where

<em>SQLITE_FILE</em>: absolute path of the sqlite file where persistent messages published on the DHT are stored.

<em>PRIVATE_KEY_FILE</em>: path to the file containing the node's private key in PEM format.

<em>IS_PERSISTENT_CACHE</em>: if set to true indicates that sifis-dht is authorized to write messages to the provided sqlite file. If set to false, the <em>SQLITE_FILE</em> content will only be used to initialize the cache.

<em>SHARED_KEY</em>: 32 bytes long shared key in hex format (command "openssl rand -hex 32" can be used to generate a random key)

<em>HTTP_PORT</em>: port to be used for the HTTP interface

<em>LOOPBACK_ONLY</em>: if set to true, only the loopback interface will be used, meaning that only other local instances of sifis-dht are discovered. If set to false, all the available network interfaces of the device will be used. Hence, two sifis-dht instances running on the same local network should discover each other.

# Acknowledgements

<!-- Links -->
[actions]: https://github.com/sifis-home/libp2p-rust-dht/actions
[codecov]: https://codecov.io/gh/sifis-home/libp2p-rust-dht
[license]: LICENSE

<!-- Badges -->
[actions badge]: https://github.com/sifis-home/libp2p-rust-dht/workflows/libp2p-rust-dht/badge.svg
[codecov badge]: https://codecov.io/gh/sifis-home/libp2p-rust-dht/branch/master/graph/badge.svg
[license badge]: https://img.shields.io/badge/license-MIT-blue.svg
