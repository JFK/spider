#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import sys
r = redis.StrictRedis()
qname = "rq:queue:" + sys.argv[1]


def purgeq(r, qname):
    while True:
        jid = r.lpop(qname)
        if jid is None:
            break
        print jid
        r.delete("rq:job:" + jid)
purgeq(r, qname)
