#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rq import Queue
from redis import Redis
from spider import Spider


def enqueue(redis, qname, pname, db, max_job_count, interval,
            wait, urls, response, referer=None, tag=0):
    conn = Redis(redis['HOST'], redis['PORT'], db=redis['DB'])
    q = Queue(qname, connection=conn)
    return q.enqueue(release, redis, qname, pname, db, max_job_count,
                     interval, wait, urls, response, referer, tag)


def release(redis, qname, pname, db, max_job_count, interval,
            wait, urls, response, referer, tag):
    s = Spider(redis, qname, pname, db, max_job_count=max_job_count,
               interval=interval, wait=wait)
    s.run(urls, response, referer=referer, tag=tag)
