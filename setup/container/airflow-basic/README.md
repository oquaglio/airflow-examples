# airflow-basic

## About

- Creates a single container
- Uses SQLite for metadata DB


## Before you Begin

Set these vars before continuing:

For 2.7.2:
AIRFLOW_IMAGE=ottoq/airflow-basic:2.7.2
AIRFLOW_CONTAINER=airflow-basic-272
AIRFLOW_PORT=9001

For 2.8.2:
AIRFLOW_IMAGE=ottoq/airflow-basic:2.8.2
AIRFLOW_CONTAINER=airflow-basic-282
AIRFLOW_PORT=9001

Verify:
echo $AIRFLOW_IMAGE
echo $AIRFLOW_CONTAINER
echo $AIRFLOW_PORT

## Build

In dir with Dockerfile:
```SH
docker build -t $AIRFLOW_IMAGE .
```

```SH
docker image ls
```

## Run [Create a New] Container

docker container run -d --name $AIRFLOW_CONTAINER -p $AIRFLOW_PORT:8080 $AIRFLOW_IMAGE

docker container run -d --restart=no --name $AIRFLOW_CONTAINER -p $AIRFLOW_PORT:8080 $AIRFLOW_IMAGE


## Start Existing Container

docker container start $AIRFLOW_CONTAINER

Wait a moment, then go to: http://localhost:9001/home

user: admin / admin


### Remove Container

```SH
docker container stop $AIRFLOW_CONTAINER; docker container rm $AIRFLOW_CONTAINER;
```
```SH
docker container ls -a
```

## Get an Interactive TTY in Container

docker exec -it $AIRFLOW_CONTAINER /bin/bash

Get root shell:

docker exec -it -u 0 $AIRFLOW_CONTAINER /bin/bash

If you need ps:

apt-get install procps


## Where are the Examples?

Get terminal to container, then:

python -c "import airflow, os; print(os.path.dirname(airflow.__file__))"

e.g.:

/usr/local/lib/python3.11/site-packages/airflow


# airflow.cfg

/opt/airflow/airflow.cfg


# Metadata DB

sudo find / -type f -name "*.db"

/opt/airflow/airflow.db

Connect:

apt-get install -y sqlite3 libsqlite3-dev

sqlite3 /opt/airflow/airflow.db


## Deploy DAGS

Optionally, remove all DAGs first:

```SH
docker exec $AIRFLOW_CONTAINER ls -l --recursive /opt/airflow/dags/
docker exec $AIRFLOW_CONTAINER find /opt/airflow/dags/ -type f
```

```SH
docker exec $AIRFLOW_CONTAINER rm -rf /opt/airflow/dags/
```

Copy single DAG:

docker cp <the file> <container-name/id>:<directory>

Copy all DAGs in dir:

docker cp dags/*.py $AIRFLOW_CONTAINER:/opt/airflow/dags

Copy all dags (run from parent dir):

find dags/ -type f -name "*.py" -exec sh -c 'docker exec 4fd3504382b9 mkdir -p "/opt/airflow/$(dirname "$0")" && docker cp "$0" 4fd3504382b9:/opt/airflow/"$0"' {} \;

Just copy the whole dags dir:

```SH
docker cp dags $AIRFLOW_CONTAINER:/opt/airflow/
```

docker cp ../../../../dags $AIRFLOW_CONTAINER:/opt/airflow/


## Tail Log Files

```SH
tail -f $AIRFLOW_HOME/logs/scheduler/latest/scheduler.log
```

```SH
docker exec -it $AIRFLOW_CONTAINER tail -f $AIRFLOW_HOME/logs/scheduler/latest/scheduler.log
```


Todos below this point...

## Deploy Plugins

docker cp plugins/ $AIRFLOW_CONTAINER:/opt/airflow/plugins


## Deploy Config

docker cp airflow.cfg $AIRFLOW_CONTAINER:/opt/airflow/airflow.cfg


## Deploy Connections

docker cp connections/ $AIRFLOW_CONTAINER:/opt/airflow/connections

## Deploy Variables

docker cp variables/ $AIRFLOW_CONTAINER:/opt/airflow/variables

## Deploy Pools

docker cp pools/ $AIRFLOW_CONTAINER:/opt/airflow/pools

## Deploy Users

docker cp users/ $AIRFLOW_CONTAINER:/opt/airflow/users

## Deploy Logs

docker cp logs/ $AIRFLOW_CONTAINER:/opt/airflow/logs

## Deploy Data

docker cp data/ $AIRFLOW_CONTAINER:/opt/airflow/data

## Get Logs

```bash
docker logs -f $AIRFLOW_CONTAINER
```