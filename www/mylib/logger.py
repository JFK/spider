#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from raven import Client
from server import settings

try:
    sentry_client = Client(settings['sentry'])
except:
    pass


def sentry():
    u"""sentryにログをポストする
    """
    try:
        exc_info = ()
        try:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc_info = (exc_type, exc_value, exc_traceback)
        except:
            pass
        kwargs = {
            'exc_info': exc_info
        }
        sentry_client.captureException(**kwargs)
    except:
        pass
