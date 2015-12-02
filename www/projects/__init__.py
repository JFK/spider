#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rq import Queue
from redis import Redis
from spider import Spider
from pymongo import MongoClient


def switch_project_db(db, pname):
    db_conn = MongoClient(db['HOST'], db['PORT'])
    return getattr(db_conn, pname)


def queue(redis):
    conn = Redis(redis['HOST'], redis['PORT'], db=redis['DB'])
    return Queue('spider', connection=conn)


def enqueue(redis, pname, db, max_job_count, interval,
            wait, urls, response, referer=None, tag=0, debug=False):
    q = queue(redis)
    if debug:
        release(redis, pname, db, max_job_count, interval,
                wait, urls, response, referer, tag, debug=debug)
    else:
        return q.enqueue(release, redis, pname, db, max_job_count,
                         interval, wait, urls, response, referer, tag)


def release(redis, pname, db, max_job_count, interval,
            wait, urls, response, referer, tag, debug=False):
    s = Spider(redis, pname, db, max_job_count=max_job_count,
               interval=interval, wait=wait, debug=debug)
    s.run(urls, response, referer=referer, tag=tag)
