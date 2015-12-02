#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import rq_settings as rq
r = redis.StrictRedis(host=rq.REDIS_HOST, port=rq.REDIS_PORT, db=rq.REDIS_DB)
qname = "rq:queue:spider"


def purgeq(r, qname):
    while True:
        jid = r.lpop(qname)
        if jid is None:
            break
        print jid
        r.delete("rq:job:" + jid)
purgeq(r, qname)
