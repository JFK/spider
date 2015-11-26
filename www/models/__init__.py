#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from mongoengine import connect


def load(db):
    u"""データベースにコネクトする
    """
    connect(
        db['name'],
        host=db['host'],
        port=db['port']
    )
