#!/usr/bin/env python
# -*- coding: utf-8 -*-


URL = 'http://snapdish.co'
DBNAME = 'spiderdb'
MAX_JOB_COUNT = 1
WAIT = 1  # seconds
INTERVAL = 86400  # 1 day


def response(soup, spider, **kwargs):
    """do some work here"""
    urls = []
    for a in soup.find_all('a'):
        urls.append(a.get('href'))
    return urls
