#! /usr/bin/env bash

# JDBC Athena driver
curl https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/AthenaJDBC42_2.0.7.jar --output AthenaJDBC42_2.0.7.jar

# Entrada
BRANCH=v0.1.3

# pcaplib4java
git clone --branch $BRANCH --depth 1 --no-checkout --filter=blob:none https://github.com/SIDN/entrada
cd entrada && git checkout $BRANCH -- pcaplib4java
cd pcaplib4java && mvn clean install

# dnslib4java
cd ../ && git checkout $BRANCH -- dnslib4java
cd dnslib4java && mvn clean install
