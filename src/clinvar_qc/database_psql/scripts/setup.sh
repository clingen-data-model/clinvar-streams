#!/bin/sh

if [ -z "$CLINVAR_DB_USER" ]; then
  echo "Must set CLINVAR_DB_USER"; exit 1
fi
if [ -z "$CLINVAR_DB_PASS" ]; then
  echo "Must set CLINVAR_DB_PASS"; exit 1
fi
db_user='postgres'
db_pass='1234'

psql