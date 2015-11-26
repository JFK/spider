#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from tornado.web import Application
from raven.contrib.tornado import AsyncSentryClient
import importlib
import yaml
import models
from tornado.options import options
import logging


def app(name, settings):
    u"""起動するアプリケーションをロードする
    """
    options.log_file_prefix = settings["logging_path"]
    logging.getLogger().setLevel(settings['logging_level'])

    models.load(settings['db'])

    with open(settings['server_apps_conf_path'], 'r') as f:
        app_conf = yaml.load(f)
        server = app_conf['common'][name]
        if settings['env'] in app_conf:
            server.update(app_conf[settings['env']][name])

    ui = {'ui_modules': {}, 'ui_methods': {}}
    for ui_key in ui.keys():
        for package in server.get(ui_key, {}).keys():
            for key in server[ui_key][package].keys():
                name = server[ui_key][package][key]
                module= importlib.import_module("mylib.%s.%s" % \
                        (ui_key, package))
                ui[ui_key].update({key: getattr(module, name)})
    settings.update(ui)

    routes = []
    for package in server['handlers'].keys():
        for uri in server['handlers'][package].keys():
            name = server['handlers'][package][uri]
            handler = importlib.import_module("handlers.%s" % package)
            routes.append((r"%s" % uri, getattr(handler, name)))

    application = Application(routes, **settings)
    try:
        application.sentry_client = AsyncSentryClient(settings['sentry'])
    except:
        pass

    return dict(
        app = application, 
        port = server['port'],
        worker = server['worker_processes']
    )
