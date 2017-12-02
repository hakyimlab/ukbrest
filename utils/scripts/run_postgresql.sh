#!/bin/bash

export PGUSER="test"
export PGPASSWORD="test"
export DB_URI="postgresql://${PGUSER}:${PGPASSWORD}@pg:5432/ukb"

docker pull postgres:9.6

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run --rm --name pg --net ukb -e POSTGRES_USER=${PGUSER} \
  -e POSTGRES_PASSWORD=${PGPASSWORD} -e POSTGRES_DB=ukb -p 5432:5432 \
  -v ${DIR}/postgresql.conf:/etc/postgresql.conf \
  postgres:9.6 postgres -c config_file=/etc/postgresql.conf

