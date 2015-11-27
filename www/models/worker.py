#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from mongoengine import Document
from mongoengine import StringField
from mongoengine import DictField
from mongoengine import ListField
from mongoengine import IntField
from mongoengine import DateTimeField

from datetime import datetime
import logging

WORKER_RUNNING = 1
WORKER_STOPPED = 0

class Worker(Document):
    pname = StringField(max_length=256, unique=True, required=True)
    dbname = StringField(max_length=256, required=True)
    qname = StringField(max_length=256, required=True)
    useragent = StringField(max_length=256, required=True)
    status = IntField(default=WORKER_RUNNING, min_value=WORKER_STOPPED,
                      max_value=WORKER_RUNNING, required=True)
    max_job_count = IntField(default=1, min_value=1, max_value=32,
                             required=True)
    wait = IntField(default=1, min_value=1, max_value=86400, required=True)
    cookies = DictField(default={}, required=True)
    jobs = ListField(default=[], required=True)
    encoding = StringField(default='utf8', max_length=256, required=True)
    interval = IntField(default=86400, min_value=3600, required=True)
    updated_at = DateTimeField(default=datetime.utcnow(), required=True)
    at = DateTimeField(default=datetime.utcnow(), required=True)
    def project(self, pname):
        try:
            return self.objects.get(pname=pname)
        except self.DoesNotExist:
            self.create_index([("pname", 1)], background=True)
            return None

    def start(self, data):
        worker = self.project(data['pname'])
        if worker:
            logging.info('update project worker... %s' % data['pname'])
            worker.updated_at = datetime.utcnow()
            self.save()
            
        else:
            logging.info('create project worker... %s' % data['pname'])
            self.pname = data['pname']
            self.dbname = data['dbname']
            self.qname = data['qname']
            self.useragent = data['useragent']
            self.max_job_count = data['max_job_count']
            self.wait = data['wait']
            self.encoding = data['encoding']
            self.interval = data['interval']
            self.validate()
            self.save()
            worker = self.project(data['pname'])
        return worker
