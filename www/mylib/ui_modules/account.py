#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from tornado.web import UIModule


class AccountErrorsModule(UIModule):
    u"""エラー処理用のモジュール
    """
    def render(self, errors):
        return self.render_string("modules/account/errors.html",
                                  errors=errors)


