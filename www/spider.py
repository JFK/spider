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
from proxy import select_proxy
import projects
import configparser
from ast import literal_eval

config = configparser.ConfigParser()
config.read('conf/spider.conf')

log_level = getattr(logging, config.get('common', 'log_level'))
logging.basicConfig(level=log_level)

debug = False

USERAGENTS = []
try:
    with open(config.get('common', 'useragents')) as fp:
        for line in fp:
            USERAGENTS.append(line.rstrip())
except:
    USERAGENTS.append('WWW-spider/1.0')

USERAGENT = random.choice(USERAGENTS)

SAVE_TEXT = literal_eval(config.get('common', 'save_text'))
SAVE_HEADERS = literal_eval(config.get('common', 'save_headers'))

DB = dict(HOST=config.get('mongodb', 'host'),
          PORT=int(config.get('mongodb', 'port')),
          DB=config.get('mongodb', 'db'))


def update_worker(pname, field, val):
    mongo = MongoClient(DB['HOST'], DB['PORT'])
    db = mongo[DB['DB']]
    cond = {'pname': pname}
    sets = {'$set': {field: val}}
    getattr(db, 'worker').update(cond, sets)
    logging.info(getattr(db, 'worker').find_one(cond))


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

    def __init__(self, redis, pname, db, max_job_count=1, wait=1,
                 qname='spider', interval=86400, encoding='utf8',
                 debug=False):
        self._qname = qname
        self._debug = debug
        self._redis = redis
        self._cookies = {}
        self._referer = None
        self._headers = {}
        self._db = db
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
                'status': 1,
                'max_job_count': max_job_count,
                'headers': {},
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

    def run(self, urls, response, referer=None, tag=0):
        for url in self.get(urls, response, referer=referer, tag=tag):
            if url:
                self.end(url)
        logging.info('finish running...')

    @property
    def redis(self):
        return self._redis

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
        return USERAGENT

    @property
    def qname(self):
        return self._qname

    @property
    def headers(self):
        # self._headers = {}
        self._headers = self.worker_one('headers', use_db=True)
        xheaders = ["X-ASN", "Content-Length", "Via", "X-ASC",
                    "Vary", "X-Request-Id", "X-XSS-Protection",
                    "X-Content-Type-Options", "X-Runtime", "ETag",
                    "Cache-Control", "Status", "X-Varnish", "Set-Cookie",
                    "Accept-Ranges", "X-Chst", "X-RealServer",
                    "Date", "Age", "Server", "Connection",
                    "X-Frame-Options", "Content-Type", "Transfer-Encoding"]
        for x in xheaders:
            if x in self._headers:
                del self._headers[x]

        if self.referer:
            self._headers.update({'Referer': self.referer})

        self._headers.update({'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'})
        self._headers.update({'Accept-Language': 'ja,en;q=0.7,en-us;q=0.3'})
        self._headers.update({'Accept-Encoding': 'gzip, deflate'})
        self._headers.update({'User-Agent': self.useragent})
        self._headers.update({'Connection': 'keep-alive'})
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
    def debug(self):
        return self._debug

    @property
    def dbname(self):
        return self._dbname

    @property
    def dbhost(self):
        return self._dbhost

    @property
    def dbport(self):
        return self._dbport

    @property
    def status(self):
        return self.worker_one('status', use_db=True)

    @property
    def max_job_count(self):
        return self.worker_one('max_job_count', use_db=True)

    @property
    def wait(self):
        return self.worker_one('wait', use_db=True)

    @property
    def jobs(self):
        return self.worker_one('jobs', use_db=True)

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
        self.sleep('end %s' % url)

    def start(self, url):
        logging.info('start... %s' % url)
        self.push_job(url)

    def worker_one(self, field_name, use_db=False):
        if use_db:
            return self.worker.find_one({'pname': self.pname})[field_name]
        else:
            return self.project_worker[field_name]

    def worker_cookies(self, c):
        c = requests.utils.dict_from_cookiejar(c)
        data = {'cookies': c}
        self.worker_update(data)

    def worker_status(self, status):
        data = {'status': status}
        self.worker_update(data)

    def worker_headers(self, headers):
        data = {'headers': headers}
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
            proxies = select_proxy()
            logging.info('proxy...')
            logging.info(proxies)
            h = self.headers
            logging.info(h)
            logging.info('cookies...')
            c = self.cookies
            logging.info(c)
            resp = requests.get(url, headers=h, cookies=c, proxies=proxies)
            resp.encoding = self.encoding
            self.worker_cookies(resp.cookies)
            resp.headers.update({'Referer': url})
            self.worker_headers(resp.headers)
            logging.info('resp.headers...')
            logging.info(resp.headers)
            if resp.status_code == 404:
                # do nothing
                logging.info('Not found... %d' % resp.status_code)

            elif resp.status_code != 200:
                logging.info('Not 200... %d' % resp.status_code)
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
                    text=resp.text if SAVE_TEXT else '',
                    headers=resp.headers if SAVE_HEADERS else {},
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
                    logging.info('No urls...')
                else:
                    i = 0
                    while len(self.jobs) > self.max_job_count:
                        if i == 10:
                            break
                        msg = 'busy... %d > %d' % \
                            (len(self.jobs), self.max_job_count)
                        i += 1
                        self.sleep(msg)
                    urls = self.clean_urls(urls)
                    logging.info('enqueue urls... %d', len(urls))
                    if i == 0:
                        projects.enqueue(self.redis, self.pname, self.db,
                                         self.max_job_count, self.interval,
                                         self.wait, urls, response,
                                         referer=self.referer,
                                         qname=self.qname, tag=tag,
                                         debug=self.debug)
                    logging.info('done!')

        except Exception, e:
            logging.warning(str(e))
            importlib.import_module('mylib.logger').sentry()

        finally:
            return url

    def sleep(self, name):
        rnd = float(random.random()) * float(self.wait)
        logging.info('Sleeping %f at %s...' % (rnd, name))
        sleep(rnd)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Spider Tools')
        parser.add_argument('--stop', action="store_true",
                            dest="stop", default=False,
                            help="stop spider")
        parser.add_argument('--start', action="store_true",
                            dest="start", default=False,
                            help="start spider")
        parser.add_argument('--debug', action="store_true",
                            dest="debug", default=False,
                            help="debug spider")
        parser.add_argument('--max-job-count', action="store", default=0,
                            help="update max job count")
        parser.add_argument('--wait', action="store", default=0,
                            help="update wait")
        parser.add_argument('-q', '--queue-name',
                            action="store", dest="q", required=False,
                            default='spider',
                            help="project module name")
        parser.add_argument('-p', '--project-name',
                            action="store", dest="p", required=True,
                            default='smaple',
                            help="project module name")
        args = parser.parse_args()

        m = importlib.import_module('projects.%s' % args.p)
        response = getattr(m, 'response')

        if args.p and args.stop:
            update_worker(args.p, 'status', 0)

        if args.p and args.start:
            update_worker(args.p, 'status', 1)

        elif args.p and args.max_job_count:
            update_worker(args.p, 'max_job_count', int(args.max_job_count))

        elif args.p and args.wait:
            update_worker(args.p, 'wait', int(args.wait))

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
            debug = args.debug
            projects.enqueue(redis, args.p, DB, max_job_count, interval,
                             wait, [url], response, qname=args.q,
                             debug=args.debug)

    except:
        importlib.import_module('mylib.logger').sentry(debug=debug)
