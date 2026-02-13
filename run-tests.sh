#!/usr/bin/env bash

set -e

# Start test broker with `docker compose up`

TEST_KAFKA_BROKER="localhost:9096" \
  dotnet test ./FsKafka.sln
