#!/usr/bin/env python
# -*- coding: utf-8 -*-

from urlparse import urlparse
import rq_settings as rq
import argparse
import requests
import requests.utils
from bs4 import BeautifulSoup
from time import sleep
import random
import logging
import importlib
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import projects

logging.basicConfig(level=logging.INFO)
USERAGENT = 'WWW-Spider/1.0'


def update_worker(db, pname, field, val):
    mongo = MongoClient(db['HOST'], db['PORT'])
    db = mongo[db['DB']]
    cond = {'pname': pname}
    sets = {'$set': {field: val}}
    getattr(db, 'worker').update(cond, sets)


class SpiderError(Exception):
    def __init__(self, value):
        self.error = value


class Spider(object):

    SPIDER = {
        'pname': '',
        'tag': 0,
        'url': '',
        'text': '',
        'referer': '',
        'headers': '',
        'encoding': '',
        'scheme': '',
        'host': '',
        'option': {},
        'query': '',
        'path': '',
        'visit_count': 1,
        'updated_at': datetime.utcnow(),
        'at': datetime.utcnow()
    }

    def __init__(self, redis, qname, pname, db, max_job_count=1,
                 wait=1, interval=86400, encoding='utf8', proxy=None):
        self._redis = redis
        self._proxy = proxy
        self._cookies = {}
        self._referer = None
        self._headers = {}
        self._db = db
        self._qname = qname
        self._pname = pname
        self._db_conn = None
        self._id = None
        self._project_worker = self.worker.find_one({'pname': pname})
        if not self._project_worker:
            logging.info('create project worker... %s' % pname)
            data = {
                'pname': pname,
                'db': db,
                'redis': redis,
                'qname': qname,
                'useragent': USERAGENT,
                'status': 1,
                'proxy': [proxy],
                'max_job_count': max_job_count,
                'cookies': {},
                'wait': wait,
                'jobs': [],
                'encoding': encoding,
                'interval': interval,
                'updated_at': datetime.utcnow(),
                'at': datetime.utcnow()
            }
            self._project_worker = data
            self._id = self.worker.insert(data)
            self._project_worker['_id'] = self._id
            self.create_index()
        else:
            logging.info('update project worker... %s' % pname)
            data = {'updated_at': datetime.utcnow()}
            self.worker_update(data)
            self._id = self._project_worker['_id']
        logging.info('worker _id: ObjectId("%s")' % self._id)

    def create_index(self):
        self.proxy.create_index([("http", DESCENDING)], background=True)
        self.worker.create_index([("pname", DESCENDING)], background=True)
        self.spider.create_index([("pname", DESCENDING)], background=True)
        self.spider.create_index([("url", DESCENDING)], background=True)
        self.spider.create_index([("at", DESCENDING), ("url", DESCENDING)],
                                 background=True)

    def get(self, urls, response, referer=None, tag=0):
        for url in urls:
            logging.info('getting... %s' % url)
            if self.is_visited(url):
                logging.info('is_visited... %s' % url)
                yield None
            else:
                logging.info('not_visited... %s' % url)
                self.start(url)
                yield self.main(url, response, referer=referer, tag=tag)
                referer = url

    def run(self, urls, response, referer=None, tag=0):
        for url in self.get(urls, response, referer=referer, tag=tag):
            if url:
                self.end(url)
        logging.info('finish running...')

    @property
    def redis(self):
        return self._redis

    @property
    def proxy(self):
        db = getattr(self.db_conn, 'proxy')
        p = map(lambda doc: doc, db.data.find({}))
        return random.choice(p) if len(p) else self._proxy

    @property
    def spider(self):
        return getattr(self.db_conn, 'spider')

    @property
    def worker(self):
        return getattr(self.db_conn, 'worker')

    @property
    def db(self):
        return self._db

    @property
    def db_conn(self):
        if not self._db_conn:
            db = MongoClient(self._db['HOST'], self._db['PORT'])
            self._db_conn = getattr(db, self._db['DB'])
        return self._db_conn

    @property
    def useragent(self):
        return self.worker_one('useragent')

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
    def pname(self):
        return self.worker_one('pname')

    @property
    def dbname(self):
        return self._dbname

    def dbhost(self):
        return self._dbhost

    @property
    def dbport(self):
        return self._dbport

    @property
    def qname(self):
        return self.worker_one('qname')

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

    @property
    def project_worker(self):
        return self._project_worker

    def pull_job(self, url):
        cond = {'_id': self._id}
        data = {'$pull': {'jobs': url}}
        self.worker.update(cond, data)

    def push_job(self, url):
        cond = {'_id': self._id}
        data = {'$push': {'jobs': url}}
        self.worker.update(cond, data)

    def end(self, url):
        self.pull_job(url)
        self.sleep('end %s' % url, self.wait)

    def start(self, url):
        logging.info('start... %s' % url)
        self.push_job(url)

    def worker_one(self, field_name, use_db=False):
        if use_db:
            return self.worker.find_one({'pname': self.pname})[field_name]
        return self.project_worker[field_name]

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

    def clean_urls(self, urls):
        start_date = datetime.utcnow() - timedelta(seconds=self.interval)
        cond = {
            'at': {'$gte': start_date},
            'url': {'$in': urls}
        }
        hits = map(lambda doc: doc.get('url', None),
                   self.spider.find(cond, projection={'url': True}))
        return set(hits) ^ set(urls)

    def visited(self, url, data):
        logging.info('visited... %s' % url)
        sets = {}
        for k in self.SPIDER:
            v = data.get(k) or self.SPIDER[k]
            if k != 'visit_count':
                sets[k] = v
        cond = {'url': url}
        sets = {'$set': sets, '$inc': {'visit_count': 1}}
        self.spider.update(cond, sets, upsert=True)

    def is_visited(self, url):
        start_date = datetime.utcnow() - timedelta(seconds=self.interval)
        cond = {
            'at': {'$gte': start_date},
            'url': url
        }
        return self.spider.find_one(cond)

    def main(self, url, response, referer=None, tag=0):
        try:
            if not self.status:
                logging.info('status stopped...')
                err = {
                    'code': 500,
                    'url': url,
                    'msg': 'status stopped...',
                    'at': datetime.utcnow()
                }
                raise SpiderError(err)
            self.referer = referer
            logging.info('referer... %s' % referer)
            logging.info('proxy...')
            logging.info(self.proxy)
            resp = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                proxies=self.proxy
            )
            resp.encoding = self.encoding
            self.worker_cookies(resp.cookies)
            if resp.status_code != 200:
                logging.info('not 200...')
                err = {
                    'code': resp.status_code,
                    'url': url,
                    'msg': 'not 200',
                    'at': datetime.utcnow()
                }
                raise SpiderError(err)
            else:
                parsed = urlparse(url)
                logging.info(parsed)
                kwargs = dict(
                    pname=self.pname,
                    url=url,
                    headers=self.headers,
                    text=resp.text,
                    encoding=self.encoding,
                    response=response,
                    scheme=parsed.scheme,
                    host=parsed.netloc,
                    query=parsed.query,
                    path=parsed.path
                )

                logging.info('response tag... %d' % tag)
                soup = BeautifulSoup(resp.text, "lxml")
                tag, urls, opt = response(self, soup, tag, **kwargs)
                kwargs.update({'tag': tag})
                kwargs.update({'option': opt})
                self.visited(url, kwargs)
                logging.info('urls... %d', len(urls))

                if not urls:
                    logging.info('not urls...')
                    err = {
                        'code': 500,
                        'url': url,
                        'msg': 'not urls',
                        'at': datetime.utcnow()
                    }
                    raise SpiderError(err)

                while len(self.jobs) > self.max_job_count:
                    self.sleep('busy... %d > %d' % (len(self.jobs),
                                                    self.max_job_count))
                urls = self.clean_urls(urls)
                logging.info('enqueue urls... %d', len(urls))
                job = projects.enqueue(self.redis, self.qname, self.pname,
                                       self.db, self.max_job_count,
                                       self.interval, self.wait, urls,
                                       response, referer=self.referer,
                                       tag=tag, proxy=self.proxy)
                logging.info('done... %d %s' % (job.result, url))

        except:
            importlib.import_module('mylib.logger').sentry()

        finally:
            return url

    def sleep(self, name, wait):
        logging.info('%s sec sleeping at %s...' % (wait, name))
        rnd1 = random.choice("2345")
        rnd2 = random.choice("6789")
        sleep(float(rnd2)/float(rnd1))
        sleep(wait)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Spider Tools')
        parser.add_argument('--stop', action="store_true",
                            dest="stop", default=False,
                            help="stop spider")
        parser.add_argument('--start', action="store_true",
                            dest="start", default=False,
                            help="start spider")
        parser.add_argument('--max-job-count', action="store", default=0,
                            help="update max job count")
        parser.add_argument('--wait', action="store", default=0,
                            help="update wait")
        parser.add_argument('-p', '--project-name',
                            action="store", dest="p", required=False,
                            help="project module name")
        parser.add_argument('-H', '--host', action="store", dest="h",
                            default='localhost', required=False,
                            help="mongodb host")
        parser.add_argument('-P', '--port', action="store", dest="port",
                            default=27017, required=False,
                            help="mongodb port")
        parser.add_argument('-d', '--db', action="store", dest="db",
                            default='spiderdb', required=False,
                            help="mongodb db")
        opts, args = parser.parse_args()

        m = importlib.import_module('projects.%s' % opts.p)
        response = getattr(m, 'response')
        db = {
            'HOST': args.h,
            'PORT': int(args.p),
            'DB': args.db
        }

        if opts.p and opts.stop:
            update_worker(db, opts.p, 'status', 0)

        if opts.p and opts.start:
            update_worker(db, opts.p, 'status', 1)

        elif opts.p and opts.max_job_count:
            update_worker(db, opts.p, 'max_job_count', int(opts.m))

        elif opts.p and opts.wait:
            update_worker(db, opts.p, 'wait', int(opts.w))

        else:
            max_job_count = getattr(m, 'MAX_JOB_COUNT')
            wait = getattr(m, 'WAIT')
            url = getattr(m, 'BASE_URL')
            interval = getattr(m, 'INTERVAL')
            redis = {
                'HOST': rq.REDIS_HOST,
                'PORT': rq.REDIS_PORT,
                'DB': rq.REDIS_DB
            }
            proxy = getattr(m, 'PROXY')
            projects.enqueue(redis, opts.p, opts.p, db, max_job_count,
                             interval, wait, [url], response, proxy=proxy)

    except:
        importlib.import_module('mylib.logger').sentry()
