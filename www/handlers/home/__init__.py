# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from handlers import BaseHandler
import tornado.auth


class HomeHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        u"""ユーザーホーム
        """
        self.render("home/index.html", me=self.me)
