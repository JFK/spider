#!/bin/bash

sudo supervisorctl start spider-worker:spider-worker-01
rqinfo
tail -f log/spider-worker-0*.log
