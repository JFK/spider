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
from bs4 import BeautifulSoup
from time import sleep
import random
import logging
import importlib
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import projects

logging.basicConfig(level=logging.INFO)

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


def enqueue(f, redis, urls, response, referer=None, tag=0, name='normal'):
    q = Queue(name, connection=redis)
    return q.enqueue(f, urls, response, referer=referer, tag=tag)


def update_wait(dbname, name, wait):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$set': {'wait': wait}}
    getattr(db, 'worker').update(cond, sets)


def update_job_count(dbname, name, count):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$set': {'max_job_count': count}}
    getattr(db, 'worker').update(cond, sets)


def exit(dbname, name):
    mongo = MongoClient()
    db = mongo[dbname]
    cond = {'name': name}
    sets = {'$set': {'status': 0}}
    getattr(db, 'worker').update(cond, sets)


class SpiderError(Exception):
    def __init__(self, value):
        self.error = value


class Spider(object):

    ERROR_LOG = {
        'err': dict,
        'at': datetime.utcnow()
    }
    WORKER = {
        'name': str,
        'useragents': list,
        'status': int,
        'max_job_count': int,
        'cookies': dict,
        'wait': int,
        'jobs': list,
        'interval': int,
        'at': datetime.utcnow()
    }
    SPIDER = {
        'tag': 0,
        'url': '',
        'text': '',
        'referer': '',
        'headers': '',
        'encoding': '',
        'scheme': '',
        'host': '',
        'query': '',
        'path': '',
        'updated': datetime.utcnow(),
        'at': datetime.utcnow()
    }

    def __init__(self, name, dbname, redis, max_job_count=1, wait=1,
                 interval=86400, encoding='utf8'):
        self._redis = Redis(redis['host'], redis['port'], db=redis['db'])
        self._cookies = dict
        self._referer = str
        self._headers = dict
        self._dbname = dbname
        self._name = name
        self._db = None
        self._id = None
        data = {
            'name': name,
            'interval': interval,
            'wait': wait,
            'status': 1,
            'cookies': {},
            'useragents': USERAGENTS,
            'jobs': [],
            'encoding': encoding,
            'max_job_count': max_job_count
        }
        self._id = self.worker.insert(data)
        logging.info('worker _id: OjbectId("%s")' % self._id)
        self.create_index()

    def create_index(self):
        self.spider.create_index([("at", DESCENDING), ("url", DESCENDING)],
                                 background=True)

    def get(self, urls, response, referer=None, tag=0):
        for url in urls:
            self.start(url)
            logging.info('request_and_parse...')
            yield self.request_and_parse(url, response, referer=referer, tag=tag)
            self.end(url)
            referer = url

    def run(self, urls, response, referer=None, tag=0):
        for url in self.get(urls, response, referer=referer, tag=tag):
            logging.info('running... %s' % url)

    @property
    def redis(self):
        return self._redis

    @property
    def spider(self):
        return getattr(self.db, 'spider')

    @property
    def worker(self):
        return getattr(self.db, 'worker')

    @property
    def error_log(self):
        return getattr(self.db, 'error_log')

    @property
    def db(self):
        if not self._db:
            mongo = MongoClient()
            self._db = getattr(mongo, self._dbname)
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
        return self.worker_one('encoding')

    @property
    def referer(self):
        return self._referer

    @property
    def useragents(self):
        return self.worker_one('useragents')

    @property
    def name(self):
        self._name = self.worker_one('name')
        return self._name

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
        return self.worker_one('cookies') or {}

    def pull_job(self, url):
        cond = {'_id': self._id}
        data = {'$pull': {'jobs': url}}
        self.worker.update(cond, data)

    def push_job(self, url):
        cond = {'_id': self._id}
        data = {'$push': {'jobs': url}}
        self.worker.update(cond, data)

    def end(self, url):
        logging.info('end: %s' % url)
        self.pull_job(url)
        self.sleep(self.wait)

    def exit(self):
        logging.info('exit...')
        self.worker_status(0)
        sys.exit(0)

    def start(self, url):
        logging.info('start: %s' % url)
        self.push_job(url)
        self.worker_status(1)

    def worker_one(self, field_name):
        return self.worker.find_one({'_id': self._id})[field_name]

    def worker_cookies(self, c):
        c = requests.utils.dict_from_cookiejar(c)
        data = {'cookies': c}
        self.worker_update(data)

    def worker_status(self, status):
        data = {'status': status}
        self.worker_update(data)

    def worker_max_job_count(self, count):
        self._max_job_count = count
        data = {'max_job_count': count}
        self.worker_update(data)

    def worker_update(self, data):
        cond = {'_id': self._id}
        sets = {'$set': data}
        self.worker.update(cond, sets)

    def err(self, err):
        self.error_log.insert(err)

    def visited(self, url, data):
        logging.info('visited...')
        sets = {}
        for k in self.SPIDER:
            v = data.get(k) or self.SPIDER[k]
            sets[k] = v
        cond = {'url': url}
        sets = {'$set': sets}
        self.spider.update(cond, sets, upsert=True)

    def is_visited(self, url):
        start_date = datetime.utcnow() - timedelta(seconds=self.interval)
        cond = {
            'at': {'$gte': start_date},
            'url': url
        }
        return self.spider.find_one(cond)

    def request_and_parse(self, url, response, referer=None, tag=0):
        try:
            self.referer = referer
            logging.info('referer: %s' % referer)
            resp = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies
            )
            resp.encoding = self.encoding
            self.worker_cookies(resp.cookies)
            if resp.status_code != 200:
                err = {
                    'code': resp.status_code,
                    'url': url,
                    'msg': 'not 200'
                }
                raise SpiderError(err)
            else:
                parsed = urlparse(url)
                logging.info(parsed)
                kwargs = dict(
                    url=url,
                    tag=tag,
                    run=self.run,
                    headers=self.headers,
                    text=resp.text,
                    encoding=self.encoding,
                    response=response,
                    scheme=parsed.scheme,
                    host=parsed.netloc,
                    query=parsed.query,
                    path=parsed.path
                )
                urls = response(
                    BeautifulSoup(resp.text, "lxml"),
                    self.spider,
                    **kwargs
                )
                self.visited(url, kwargs)
                if not urls:
                    err = {
                        'code': 500,
                        'url': url,
                        'msg': 'not urls'
                    }
                    raise SpiderError(err)

                if not self.status:
                    err = {
                        'code': 500,
                        'url': url,
                        'msg': 'not status'
                    }
                    raise SpiderError(err)

                while len(self.jobs) > self.max_job_count:
                    logging.info('busy...%d %d' % (len(self.jobs),
                                                   self.max_job_count))
                    self.sleep(self.wait)

                logging.info('enqueue...')
                job = enqueue(self.run, self.redis, urls, response,
                              referer=self.referer, tag=tag)

                while not job.result:
                    logging.info('done...%d' % job.result)
                    self.sleep(self.wait)

        except SpiderError, err:
            logging.info(str(err))
            self.err(err)

        except:
            importlib.import_module('mylib.logger').sentry()

        finally:
            return url

    def sleep(self, wait):
        rnd1 = random.choice("2345")
        rnd2 = random.choice("6789")
        sleep(float(rnd2)/float(rnd1))
        sleep(wait)

if __name__ == '__main__':
    try:
        parser = optparse.OptionParser()
        parser.add_option('-e', '--exit', action="store_true",
                          dest="exit", default=False, help="exit: -r required")
        parser.add_option('-m', '--max-job-count',
                          action="store", dest="m", default=0,
                          help="update max job count: ex. -r name -m 1")
        parser.add_option('-w', '--wait',
                          action="store", dest="w", default=0,
                          help="update wait: ex. -r name -w 1")
        parser.add_option('-r', '--project-name',
                          action="store", dest="r",
                          help="project module name")
        parser.add_option('-q', '--queue',
                          action="store", dest="q", default='normal',
                          help="queue name[high|normal|law]")
        opts, args = parser.parse_args()
        if opts.r and opts.exit:
            m = importlib.import_module('projects.%s' % opts.r)
            dbname = getattr(m, 'DBNAME')
            exit(dbname, opts.r)

        elif opts.r and opts.m:
            m = importlib.import_module('projects.%s' % opts.r)
            dbname = getattr(m, 'DBNAME')
            update_job_count(dbname, opts.r, opts.m)

        elif opts.r and opts.w:
            m = importlib.import_module('projects.%s' % opts.r)
            dbname = getattr(m, 'DBNAME')
            update_job_count(dbname, opts.r, opts.w)

        else:
            m1 = importlib.import_module('projects')
            start = getattr(m1, 'start')

            m2 = importlib.import_module('projects.%s' % opts.r)
            response = getattr(m2, 'response')
            dbname = getattr(m2, 'DBNAME')
            max_job_count = getattr(m2, 'MAX_JOB_COUNT')
            wait = getattr(m2, 'WAIT')
            url = getattr(m2, 'URL')
            interval = getattr(m2, 'INTERVAL')
            redis_host = getattr(m2, 'REDIS_HOST')
            redis_port = getattr(m2, 'REDIS_PORT')
            redis_db = getattr(m2, 'REDIS_DB')
            redis = Redis(redis_host, redis_port, db=redis_db)
            q = Queue(opts.q, connection=redis)
            q.enqueue(projects.start, opts.r, dbname, redis_host,
                      redis_port, redis_db, max_job_count,
                      interval, wait, [url], response)

    except:
        importlib.import_module('mylib.logger').sentry()
