#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rq_settings as rq
import re
import logging

BASE_URL = 'http://snapdish.co/books/'
DBNAME = 'spiderdb'
MAX_JOB_COUNT = 1
WAIT = 1  # seconds
INTERVAL = 86400  # 1 day
REDIS = {
    'HOST': rq.REDIS_HOST,
    'PORT': rq.REDIS_PORT,
    'DB': rq.REDIS_DB
}


def response(spider, soup, tag, **kwargs):
    logging.info('sample response...')
    urls = []
    for a in soup.find_all('a'):
        href = a.get('href')
        if href and re.match('/books/', href) and href != '/books/':
            urls.append('http://snapdish.co%s' % href)
    return urls
