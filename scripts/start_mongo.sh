#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DB_PATH="$DIR/../data"

mongod --bind_ip 127.0.0.1 --dbpath $DB_PATH
