#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from handlers import BaseHandler


class TopHandler(BaseHandler):
    def get(self):
        u"""トップページ
        """
        self.render("top/index.html")
