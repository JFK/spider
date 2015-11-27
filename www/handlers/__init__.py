#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

import sys
import logging
import time
from hashlib import md5
try:
    from urllib import parse
except:
    import urllib as parse
from tornado.web import RequestHandler
from models.user import User
from raven.contrib.tornado import SentryMixin


class ValidationError(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return self.code


class BaseHandler(SentryMixin, RequestHandler):
    ALLOW_VALID_QUERY_PARAMETERS = True
    VALID_QUERY_PARAMETERS = ()

    def get_current_user(self):
        u"""現在のログインユーザー名を取得をsecure_cookieから取得して返す関数

        :return: ユーザー名
        :rtype: string

        ... note:: クッキー名が取得できない場合、 メールアドレスのアカウント
        名を返す
        """
        username = self.get_secure_cookie(
            self.settings["current_user_cookie_name"]
        )
        if not username and self.email:
            username = self.email.split("@")[0]
        return username

    @property
    def email(self):
        u"""メールアドレスプロパティ

        :rtype: string
        """
        if not hasattr(self, '_email'):
            email = self.get_secure_cookie(self.settings["token_email_name"])
            _email = ''
            if email:
                _email = email.decode('utf-8')
            self._email = _email
        return self._email

    @property
    def password(self):
        u"""パスワードプロパティ

        :rtype: string
        """
        if not hasattr(self, '_password'):
            tk = self.get_secure_cookie(self.settings["token_name"])
            _password = ''
            if tk and self.email and self.md5(self.email.encode('utf-8')) == \
                    tk[32:].decode('utf-8'):
                _password = tk[:32].decode('utf-8')
            self._password = _password
        return self._password

    @property
    def me(self):
        u"""ログインユーザーのUserデーターオブジェクト

        :rtype: User(mongoengine.Document) Object
        """
        if not hasattr(self, '_me'):
            self._me = None
            if self.email and self.password:
                _me = User.objects.get(
                    email=self.email,
                    password=self.password
                )
            self._me = _me
        return self._me

    def md5(self, text):
        u"""md5ラッパー関数

        :rtype: string
        """
        return md5(text).hexdigest()

    def load_ui_modules(self, ui_modules):
        u"""ui_moduleを読み込む関数

        :param arg1: ui_modulesの一覧
        :type arg1: dict

        :Example:

        >>> class ErrorsModule(UIModule):
        >>>     def render(self, errors):
        >>>         return self.render_string(
        >>>             "account/modules/errors.html",
        >>>             errors=errors
        >>>         )
        >>> ...
        >>> ...
        >>> def initialize(self):
        >>>    self.load_ui_modules({"Errors": ErrorsModule})

        ... note:: initialize内で設定する
        """
        self.ui["modules"].handler = self
        self.ui["modules"].ui_modules.update(ui_modules)

    def create_token(self, password, email):
        u"""トークンを生成する関数

        :param arg1: 入力された md5(password)
        :param arg2: 入力された email
        :type arg1: string
        :type arg2: string

        :return: トークン
        :rtype: string
        """
        return password + self.md5(email.encode('utf-8'))

    def go_to_home(self):
        u"""ユーザーのホームページにリダイレクト
        """
        if self.current_user:
            self.redirect("/home")

    def username(self, last_name, first_name):
        u"""ユーザー名クッキーに設定

        :param arg1: 姓
        :param arg2: 名
        :type arg1: string
        :type arg2: string

        ... note:: 引数の値をからにして渡すと削除する
        """
        if first_name and last_name:
            self.current_user = "%s %s" % (first_name, last_name)
            self.set_secure_cookie(
                self.settings["current_user_cookie_name"],
                self.current_user
            )
        else:
            self.clear_cookie(self.settings["current_user_cookie_name"])

    def login(self, password, email):
        u"""ログイン処理

        :param arg1: password
        :param arg2: email

        ... note:: クッキーの保持期間30日
        """
        token = self.create_token(password, email)
        self.set_secure_cookie(
            self.settings["token_name"],
            token,
            expires_days=30
        )
        self.set_secure_cookie(
            self.settings["token_email_name"],
            email,
            expires_days=30
        )

    def logout(self, redirect_to="/"):
        u"""ログアウト処理

        :param arg1: redirect_to ログアウト処理後のリダイレクト先のURL
        """
        self.clear_cookie(self.settings["token_name"])
        self.clear_cookie(self.settings["token_email_name"])
        self.clear_cookie(self.settings["current_user_cookie_name"])
        self.redirect(redirect_to)

    def debug(self, message):
        u"""デバッグメッセージをログに吐く

        :param arg1: メッセージ
        :type arg1: string
        """
        logging.debug('%s', message)

    def warning(self, message):
        u"""ワーニングメッセージをログに吐く

        :param arg1: メッセージ
        :type arg1: string
        """
        logging.warning('%s', message)

    def exception(self, err):
        u"""エクセプションをスタックトレースする

        :param arg1: エクセプションハンドラー
        :type arg1: Object
        """
        logging.exception(err)

    def error(self):
        u"""エラーのスタックトレース
        """
        self.log_exception(*sys.exc_info())

    def start_time(self, message=''):
        u"""処理速度の測定開始

        :param arg1: message クラス名など指定するとわかりやすい

        :Example:

        >>> self.start_time(self.__class__.__name__)

        ... note:: debugでモードでログに出力される
        """
        self.t = time.time()
        self.debug('start time... %s', message)

    def end_time(self):
        u"""処理速度の測定終了

        :Example:

        >>> self.end_time()

        ... note:: debugでモードでログに出力される
        """
        ms = round((time.time() - self.t)*1000, 3)
        self.debug('end time %sms' % ms)

    def validate_query(self):
        u"""GETのクエリーを制御する
        """
        if self.ALLOW_VALID_QUERY_PARAMETERS and \
                self.request.method.lower() == 'get':
            base_url = self.request.uri.split('?')[0]
            query = self.request.query
            vqp = set(self.VALID_QUERY_PARAMETERS)
            if not vqp and query:
                self.redirect(base_url, permanent=True)
                return

            rqk = self.request.arguments.keys()
            params = {}
            for k in rqk:
                if self.get_argument(k, None):
                    params.update({k: self.get_argument(k)})
            expected_query = parse.urlencode(params)

            if expected_query != query:
                url = '%s?%s' % (base_url, expected_query)
                self.redirect(url, permanent=True)
                return

            pk = set(params.keys())
            invalid = (vqp ^ pk) & pk
            if invalid:
                for x in invalid:
                    params.pop(x, None)
                expected_query = parse.urlencode(params)
                url = '%s?%s' % (base_url, expected_query)
                self.redirect(url, permanent=True)
                return

    def initialize(self):
        u"""初期化関数
        """
        self.debug(u"initialize...")

    def prepare(self):
        u"""handler起動前に呼ばれる関数
        """
        self.validate_query()
        self.debug(u"prepare...")

    def on_finish(self):
        u"""handler起動後に呼ばれる関数
        """
        self.debug(u"on_finish...")
