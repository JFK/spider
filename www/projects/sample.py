#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rq_settings as rq
import re
import logging
from projects import queue as q

BASE_URL = 'http://snapdish.co/books/'

MAX_JOB_COUNT = 1
WAIT = 1  # seconds
INTERVAL = 86400  # 1 day
REDIS = {
    'HOST': rq.REDIS_HOST,
    'PORT': rq.REDIS_PORT,
    'DB': rq.REDIS_DB
}
MONGODB = {
    'HOST': 'localhost',
    'PORT': 27017,
    'DB': 'spiderdb'
}

def keyword(text):
    logging.info(text)

def response(spider, soup, tag, **kwargs):
    logging.info('sample response...')
    urls = []
    option = {}
    for a in soup.find_all('a'):
        href = a.get('href')
        if href and re.match('/books/', href) and href != '/books/':
            urls.append('http://snapdish.co%s' % href)
            q(spider.redis, qname='normal').enqueue(keyword, a.text)
    return (tag, urls, option)
