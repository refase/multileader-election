#!/bin/bash
docker build -t ratnadeepb/rdb-election -f Dockerfile .
docker push ratnadeepb/rdb-election:latest