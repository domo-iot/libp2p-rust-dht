#!/bin/bash

# DoMO gateway target
cross build --release --target aarch64-unknown-linux-musl --features=vendored

# amd64/alpine
cross build --release --target x86_64-unknown-linux-musl --features=vendored

