#!/bin/bash

CURL=/usr/bin/curl
port=9200
host=localhost

$CURL -XDELETE -H "Content-Type: application/json; charset=UTF-8" http://$host:$port/smoothed
$CURL -XDELETE -H "Content-Type: application/json; charset=UTF-8" http://$host:$port/triangle

$CURL -XPUT -H "Content-Type: application/json; charset=UTF-8" http://$host:$port/smoothed -d @settings.json
$CURL -XPUT -H "Content-Type: application/json; charset=UTF-8" http://$host:$port/triangle -d @settings.json

