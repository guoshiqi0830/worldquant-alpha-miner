#!/bin/bash

DEFAULT_DB="db/worldquant.db"

if [ -z "$1" ]; then
    DB_FILE="$DEFAULT_DB"
else
    DB_FILE="$1"
fi

if [ -f "$DB_FILE" ]; then
    echo "warning $DB_FILE exists!"
    read -p "overwrite? (y/n) " REPLY
    echo
    if [ "$REPLY" != "y" ] && [ "$REPLY" != "Y" ]; then
        echo "done."
        exit 1
    fi
    rm -f "$DB_FILE"
fi

echo "creating $DB_FILE ..."

INIT_SQL=$(cat db/schema.sql)

if command -v sqlite3 >/dev/null 2>&1; then
    echo "$INIT_SQL" | sqlite3 "$DB_FILE"
    if [ $? -eq 0 ]; then
        echo "database $DB_FILE created!"
    else
        echo "error fail to init db!"
        exit 1
    fi
else
    echo "please install sqlite3"
    exit 1
fi