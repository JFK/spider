#!/usr/bin/env python
# -*- coding: utf-8 -*-

from spider import Spider


def start(name, dbname, redis_host, redis_port, redis_db,
          max_job_count, interval, wait, urls, response,
          referer=None, tag=0):
    redis = {
        'host': redis_host,
        'port': redis_port,
        'db': redis_db,
    }
    s = Spider(name, dbname, redis, max_job_count=max_job_count,
               interval=interval, wait=wait)
    s.run(urls, response, referer=referer, tag=tag)
