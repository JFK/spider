#!/bin/bash

rqinfo
sudo supervisorctl stop spider-worker:spider-worker-01
redis-cli flushall
echo '' | sudo tee log/spider-worker-0*
