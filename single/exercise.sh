#!/bin/sh
cd kafka-2.13_2.7.1
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092
