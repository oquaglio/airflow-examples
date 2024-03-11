#!/usr/bin/env bash

# Environment variables
export DATABASE_NAME=db

# Initiliase the metastore
airflow db init

#sed -i 's/dags_are_paused_at_creation = True/dags_are_paused_at_creation = False/' ${AIRFLOW_HOME}/airflow.cfg

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Create a user (if you want a user to access the web interface)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@admin.com \
    --password admin

# Run the web server in foreground (for docker logs)
exec airflow webserver
