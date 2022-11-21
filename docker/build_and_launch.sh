#!/bin/bash
docker build -t sifis-dht .
docker run --net=host --name sifis-dht --rm sifis-dht:latest
