#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rq import Queue
from redis import Redis
from spider import Spider


def queue(redis, qname='normal'):
    conn = Redis(redis['HOST'], redis['PORT'], db=redis['DB'])
    return Queue(qname, connection=conn)


def enqueue(redis, qname, pname, db, max_job_count, interval,
            wait, urls, response, referer=None, tag=0, proxy=None):
    q = queue(redis, qname)
    return q.enqueue(release, redis, qname, pname, db, max_job_count,
                     interval, wait, urls, response, referer, tag, proxy)


def release(redis, qname, pname, db, max_job_count, interval,
            wait, urls, response, referer, tag, proxy):
    s = Spider(redis, qname, pname, db, max_job_count=max_job_count,
               interval=interval, wait=wait, proxy=proxy)
    s.run(urls, response, referer=referer, tag=tag)
