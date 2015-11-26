#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from rq import Queue
from urlparse import urlparse
import optparse
from redis import Redis
from random import randint
import requests
import requests.utils
import pickle
from bs4 import BeautifulSoup
from io import StringIO
from time import sleep
import random
import logging
import importlib
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId
from datetime import datetime, timedelta


USERAGENTS = [
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.0.3705; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; MyIE2; Maxthon; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; Q312461; FunWebProducts; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; Woningstichting Den Helder; .NET CLR 1.0.3705)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.41115)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; MyIE2; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; MyIE2; Maxthon; .NET CLR 1.1.4322)'
]


def enqueue(f, urls, response, referer=None, tag=0, name='normal'):
    q = Queue(name, connection=Redis())
    return q.enqueue(f, urls, response, referer=referer, tag=tag)


def update_wait(dbname, name, wait):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$sets': {'wait': wait}}
    getattr(db, 'worker').update(cond, sets)

def update_job_count(dbname, name, count):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$sets': {'max_job_count': count}}
    getattr(db, 'worker').update(cond, sets)


def exit(dbname, name):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$sets': {'status': 0}}
    getattr(db, 'worker').update(cond, sets)


class SpiderError(Exception):
    def __init__(self, value):
        self.error = value


class Spider(object):

    WORKER = {
        'name': str,
        'dbname': str,
        'useragents': list,
        'status': int,
        'max_job_count': int,
        'wait': int,
        'jobs': list,
        'interval': int,
        'at': datetime.utcnow()
    }
    SPIDER = {
        '_id': ObjectId,
        'tag': int,
        'url': str,
        'text': str,
        'referer': dict,
        'encoding': str,
        'scheme': str,
        'host': str,
        'query': str,
        'path': str,
        'updated': datetime.utcnow(),
        'at': datetime.utcnow()
    }

    def __init__(self, name, dbname, wait=1, max_job_count=2,
                 interval=86400, encoding='utf8'):
        self._cookies_stream = StringIO()
        self._referer = str
        self._headers = dict
        data = {
            'name': name,
            'dbname': dbname,
            'interval': interval,
            'wait': wait,
            'useragents': USERAGENTS,
            'encoding': encoding,
            'max_job_count': max_job_count
        }
        self.worker_update(self.validate_data('worker', data))
        self.create_index()

    def validate_data(self, name, data):
        validated_data = {}
        for k, v in enumerate(data):
            sv = getattr(self, name.upper()).get(k, None)
            if sv and isinstance(sv, type(v)):
                v = sv
            validated_data.update({k, sv})
        return validated_data

    def create_index(self):
        self.spider.create_index([("at", DESCENDING), ("url", DESCENDING)],
                                 background=True)

    def get(self, url, response, referer=None, tag=0):
        yield self.request_and_parse(url, response, referer=referer, tag=tag)

    def run(self, urls, response, referer=None, tag=0):
        for url in urls:
            try:
                self.start(url)
                self.get(url, response, referer=referer, tag=tag).next()
                referer = url

            except:
                importlib.import_module('mylib.logger').sentry()

            finally:
                self.end(url)

    @property
    def spider(self):
        return getattr(self.db, 'spider')

    @property
    def worker(self):
        return getattr(self.db, 'worker')

    @property
    def db(self):
        if not self._db:
            mongo = MongoClient()
            self._db = mongo[self.dbname]
        return self._db

    @property
    def useragent(self):
        rnd = randint(0, len(self.useragents) - 1)
        return self.useragents[rnd]

    @property
    def headers(self):
        self._headers = {
            'User-agent': self.useragent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en;q=0.7,en-us;q=0.3',
        }
        self._headers.update({'referer': self.referer})
        self._headers.update({'encoding': self.encoding})
        return self._headers

    @property
    def encoding(self):
        return self._encoding

    @property
    def referer(self):
        return self._referer

    @property
    def useragents(self):
        return self.worker_one('useragents')

    @property
    def name(self):
        return self.worker_one('name')

    @property
    def dbname(self):
        return self.worker_one('dbname')

    @property
    def status(self):
        return self.worker_one('status')

    @property
    def max_job_count(self):
        return self.worker_one('max_job_count')

    @property
    def wait(self):
        return self.worker_one('wait')

    @property
    def jobs(self):
        return self.worker_one('jobs')

    @property
    def interval(self):
        return self.worker_one('interval')

    @referer.setter
    def referer(self, value):
        self._referer = value

    @property
    def cookies(self):
        value = self._cookies_stream.getvalue()
        if value:
            return requests.utils.cookiejar_from_dict(
                pickle.loads(value.encode('utf-8'))
            )
        else:
            return requests.utils.cookiejar_from_dict({})

    def write_cookies(self):
        session = requests.session()
        cookiejar = requests.utils.dict_from_cookiejar(session.cookies)
        self._cookies_stream.write(unicode(pickle.dumps(cookiejar)))

    def pull_job(self, url):
        data = {'$push': {'jobs': url}}
        self.worker_update(data)

    def push_job(self, url):
        data = {'$push': {'jobs': url}}
        self.worker_update(data)

    def end(self, url):
        logging.warning('end: %s' % url)
        self.pull_job(url)
        self.sleep(self.wait)

    def exit(self):
        logging.warning('exit...')
        self.worker_status(0)
        sys.exit(0)

    def start(self, url):
        logging.warning('start: %s' % url)
        self.push_job(url)
        self.worker_status(1)

    def worker_one(self, field_name):
        return self.worker.find_one({'name': self.name})[field_name]

    def worker_status(self, status):
        data = self.validate_data('worker', {'status': status})
        self.worker_update(data)

    def worker_max_job_count(self, count):
        self._max_job_count = count
        data = self.validate_data('worker', {'max_job_count': count})
        self.worker_update(data)

    def worker_update(self, data):
        cond = {'name': self.name}
        sets = {'$set': data}
        self.worker.update(cond, sets, upsert=True)

    def visited(self, data):
        data = self.validate_data('spider', data)
        if self.is_visited(data.get('url')):
            del data['at']
        self.spider.update({'url': url}, {'$set': data}, upsert=True)

    def is_visited(self, url):
        start_date = datetime.utcnow() - timedelta(seconds=self.interval)
        cond = {
            'at': {'$gte': start_date},
            'url': url
        }
        return self.spider.find_one(cond)

    def request_and_parse(self, url, response, referer=None, tag=0):
        self.referer = referer
        resp = requests.get(
            url,
            headers=self.headers,
            cookies=self.cookies
        )
        resp.encoding = self.encoding
        self.write_cookies()
        if resp.status_code == 200:
            try:
                parsed = urlparse(url)
                kwargs = dict(
                    url=url,
                    tag=tag,
                    run=self.run,
                    headers=self.headers,
                    text=resp.text,
                    encoding=self.encoding,
                    response=response,
                    scheme=parsed.scheme,
                    host=parsed.host,
                    query=parsed.query,
                    path=parsed.path
                )
                urls = response(
                    BeautifulSoup(resp.text, "lxml"),
                    self.spider,
                    **kwargs
                )
                self.visited(kwargs)
                if not urls or not self.status:
                    self.end(url)

                while len(self.jobs) >= self.max_job_count:
                    self.sleep(self.wait)

                job = enqueue(self.run, urls, response,
                              referer=self.referer, tag=tag)

                while not job.result:
                    self.sleep(self.wait)

            except Exception, err:
                err = {
                    'code': 500,
                    'url': url,
                    'msg': str(err)
                }
                raise SpiderError(err)

        else:
            err = {
                'code': resp.status_code,
                'url': url,
                'msg': 'Response error'
            }
            raise SpiderError(err)

    def sleep(self):
        rnd1 = random.choice("2345")
        rnd2 = random.choice("6789")
        sleep(float(rnd2)/float(rnd1))

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-e', '--exit', action="store_true",
                      dest="exit", default=True, help="exit: -r required")
    parser.add_option('-m', '--max-job-count',
                      action="store", dest="m",
                      help="update max job count: -r required: ex. -r name -m 1")
    parser.add_option('-w', '--wait',
                      action="store", dest="m",
                      help="update wait: -r required: ex. -r name -w 1")
    parser.add_option('-r', '--project-name',
                      action="store", dest="r",
                      help="project module name")
    parser.add_option('-q', '--queue',
                      action="store", dest="q", default='normal',
                      help="queue name[high|normal|law]")
    opts, args = parser.parse_args()
    if opts.r and opts.exit:
        m = importlib.import_module('project.%s' % opts.r)
        dbname = getattr(m, 'DBNAME')
        exit(dbname, opts.r)
        sys.exit(0)

    elif opts.r and opts.m:
        m = importlib.import_module('project.%s' % opts.r)
        dbname = getattr(m, 'DBNAME')
        update_job_count(dbname, opts.r, opts.m)
        sys.exit(0)

    elif opts.r and opts.w:
        m = importlib.import_module('project.%s' % opts.r)
        dbname = getattr(m, 'DBNAME')
        update_job_count(dbname, opts.r, opts.w)
        sys.exit(0)

    else:
        m1 = importlib.import_module('project')
        start = getattr(m1, 'start')

        m2 = importlib.import_module('project.%s' % opts.r)
        response = getattr(m2, 'response')
        dbname = getattr(m2, 'DBNAME')
        max_job_count = getattr(m2, 'MAX_JOB_COUNT')
        wait = getattr(m2, 'WAIT')
        url = getattr(m2, 'URL')
        interval = getattr(m2, 'INTERVAL')

        enqueue(start, opts.r, dbname, max_job_count, interval
                [url], response, name=opts.q)
        sys.exit(0)
