#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "usage: hard_reset user dbname"
    exit 1
fi

echo "Dropping Postgres database $2..."
dropdb $2

echo "Recreating Postgres database $2..."
createdb -U $1 $2

psql $1 $2 << EOF
    GRANT ALL PRIVILEGES ON DATABASE $2 TO $1;
EOF
