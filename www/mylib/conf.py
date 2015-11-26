#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com

from logging import WARNING, DEBUG
import yaml
import os.path


def load():
    u"""サーバーとアプリの環境設定を読み込む
    """
    env = 'development' if not os.environ.get('APP_NAME') else \
        os.environ.get('APP_NAME')
    base_path = os.path.dirname(os.path.dirname(__file__))
    conf_path = '%s/%s' % (base_path, "conf/server.yml")
    with open(conf_path) as f:
        server_conf = yaml.load(f)
        server_conf['common'].update(server_conf[env])
        logging_level = DEBUG if server_conf['common']['debug'] \
            else WARNING
        server_conf['common'].update({'logging_level': logging_level})
        server_conf['common'].update({'env': env})
        return server_conf['common']
