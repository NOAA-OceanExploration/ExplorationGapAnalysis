#!/bin/bash

# init script for airflow setup
mkdir -p ./dags ./logs ./plugins ./config ./database ./bucket ./sparkapps # only needed if you didn't clone the dirs
sudo usermod -aG docker $USER
echo -e "AIRFLOW_UID=$(id -u)" > .env

#docker compose up airflow-init # initialize database, should exit with code 0

#docker compose down --volumes --remove-orphans # cleanup after running if you have big problems

#AWS issues
echo "fs.inotify.max_user_watches=1048576" >> /etc/sysctl.conf sysctl -p
sudo usermod -aG docker ssm-user
