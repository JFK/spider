#!/usr/bin/env python
# -*- coding: utf-8 -*-

import conf.rq as rq
import logging

URL = 'http://snapdish.co'
DBNAME = 'spiderdb'
MAX_JOB_COUNT = 1
WAIT = 1  # seconds
INTERVAL = 86400  # 1 day
REDIS_HOST = rq.REDIS_HOST
REDIS_PORT = rq.REDIS_PORT
REDIS_DB = rq.REDIS_DB


def response(soup, spider, **kwargs):
    """do some work here"""
    urls = []
    for a in soup.find_all('a'):
        urls.append(a.get('href'))
    logging.warning(urls)
    return urls
