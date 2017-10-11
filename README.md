# content-exporter

[![Circle CI](https://circleci.com/gh/Financial-Times/content-exporter/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/content-exporter/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/content-exporter)](https://goreportcard.com/report/github.com/Financial-Times/content-exporter) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/content-exporter/badge.svg)](https://coveralls.io/github/Financial-Times/content-exporter)

## Introduction

The service is used for automated content exports. There are 3 types of export:
* A *FULL export* consists in inquiring data from DB, calling /enrichedcontent endpoint for the obtained data and uploading it to S3 via the Data RW S3 service.
* An *INCREMENTAL export* consists in listening on the notification topic and taking action for the content, according to the notification event type:
    * if it's an UPDATE event then calling /enrichedcontent endpoint for the obtained data and uploading it to S3 via the Data RW S3 service
    * if it's a DELETE event then deleting the content from S3 via the Data RW S3 service
* A *TARGETED export* is similar to the FULL export but triggering only for specific data

Normally an `INCREMENTAL export` is happening as the service starts consuming messages from Kafka at the startup.

## Installation

Download the source code, dependencies and test dependencies:

        go get -u github.com/kardianos/govendor
        go get -u github.com/Financial-Times/content-exporter
        cd $GOPATH/src/github.com/Financial-Times/content-exporter
        govendor sync
        go build .

## Running locally

1. Run the tests and install the binary:

        govendor sync
        govendor test -v -race
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/content-exporter [--help]

Usage: content-exporter [OPTIONS]

        Exports content from DB and sends to S3

        Options:
          --app-system-code="content-exporter"                       System Code of the application ($APP_SYSTEM_CODE)
          --app-name="content-exporter"                              Application name ($APP_NAME)
          --port="8080"                                              Port to listen on ($APP_PORT)
          --mongoConnection=""                                       Mongo addresses to connect to in format: host1:port1,host2:port2,...] ($MONGO_CONNECTION)
          --enrichedContentBaseURL="http://localhost:8080"           Base URL to enriched content endpoint ($ENRICHED_CONTENT_BASE_URL)
          --enrichedContentHealthURL="http://localhost:8080/__gtg"   Health URL to enriched content endpoint ($ENRICHED_CONTENT_HEALTH_URL)
          --s3WriterBaseURL="http://localhost:8080"                  Base URL to S3 writer endpoint ($S3_WRITER_BASE_URL)
          --s3WriterHealthURL="http://localhost:8080/__gtg"          Health URL to S3 writer endpoint ($S3_WRITER_HEALTH_URL)
          --xPolicyHeaderValues=""                                   Values for X-Policy header separated by comma, e.g. INCLUDE_RICH_CONTENT,EXPAND_IMAGES ($X_POLICY_HEADER_VALUES)
          --authorization=""                                         Authorization for enrichedcontent endpoint, needed only when calling the endpoint via Varnish ($AUTHORIZATION)
          --kafka-addr=""                                            Comma separated kafka hosts for message consuming. ($KAFKA_ADDRS)
          --group-id=""                                              Kafka qroup id used for message consuming. ($GROUP_ID)
          --topic=""                                                 Kafka topic to read from. ($TOPIC)
          --delayForNotification=30                                  Delay in seconds for notifications to being handled ($DELAY_FOR_NOTIFICATION)
          --whitelist=""                                             The whitelist for incoming notifications - i.e. ^http://.*-transformer-(pr|iw)-uk-.*\.svc\.ft\.com(:\d{2,5})?/content/[\w-]+.*$ ($WHITELIST)
          --logDebug=false                                           Flag to switch debug logging ($LOG_DEBUG)
3. Test:

           curl http://localhost:8080/__health

## Build and deployment

* Built by Docker Hub on merge to master: [coco/content-exporter](https://hub.docker.com/r/coco/content-exporter/)
* CI provided by CircleCI: [content-exporter](https://circleci.com/gh/Financial-Times/content-exporter)

## Service endpoints

HTTP Endpoints are only for FULL and TARGETED exports

### POST
* `/export` - Triggers an export. If `ids` is in the json body request, then a TARGETED export is triggered, otherwise a FULL export
### GET
* `/jobs` - Returns all the running jobs
* `/jobs/{jobID}` - Returns the job specified by the `jobID` parameter

## Healthchecks
Admin endpoints are:

`/__gtg`

`/__health`

`/__build-info`

There are several checks performed:

* Checks that a connection can be made to Kafka, using the kafka specific configuration supplied in service startup.
* Checks that a connection can be made to Mongo, using the mongo specific configuration supplied in service startup.
* Checks that the enriched content fetcher service is healthy
* Checks that the S3 updater service is healthy

### Logging

* The application uses [logrus](https://github.com/Sirupsen/logrus); the log file is initialised in [main.go](main.go).
* NOTE: `/__build-info` and `/__gtg` endpoints are not logged as they are called every second from varnish/vulcand and this information is not needed in logs/splunk.
