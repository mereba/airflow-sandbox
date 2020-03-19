#!/usr/bin/env sh
AIRFLOW__CORE__EXECUTOR=${EXECUTOR}
WEBSERVER_PORT=${WEBSERVER_PORT}

case "$2" in
  webserver)
    airflow initdb
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      airflow scheduler &
      airflow webserver
    fi
    ;;
  *)
    echo "INFO: parameter passed: $2"
    exec "$@"
    ;;
esac
