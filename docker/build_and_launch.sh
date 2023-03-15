#!/bin/bash
docker build -t alpine-dht .
docker run --net=host --name alpine-dht --rm alpine-dht:latest
