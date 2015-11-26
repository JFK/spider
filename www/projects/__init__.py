#!/usr/bin/env python
# -*- coding: utf-8 -*-

from spider import Spider


def start(name, dbname, max_job_count, interval,
          urls, response, referer=None, tag=0):
    s = Spider(name, dbname, max_job_count, interval)
    s.run(urls, response, referer=referer, tag=tag)
