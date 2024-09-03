# airflow-examples

## Start

Spin up one of the environments in /setup.


## Install build pre-reqs

Fedora:

Try this first:

sudo dnf update -y && dnf install -y @development-tools

More stuff:

sudo dnf update -y && \
    dnf install -y \
    wget \
    czmq-devel \
    curl \
    openssl-devel \
    git \
    telnet \
    bind-utils freetds-devel \
    krb5-devel \
    cyrus-sasl-devel \
    libffi-devel libpq-devel \
    freetds \
    gcc-c++ \
    mariadb-devel \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    glibc-locale-source glibc-langpack-en \
    procps-ng \
    sqlite sqlite-devel \
    && dnf clean all


## Setup local testing virual env

pyenv virtualenv 3.11.8 airflow

pyenv local activate

pyenv activate

pip install --upgrade pip setuptools wheel

AIRFLOW_VERSION=2.8.2
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint ../../setup/container/airflow-basic/2.8.2/constraints.txt


## Now run the dags

python dag.py