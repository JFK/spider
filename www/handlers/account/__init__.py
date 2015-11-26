#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 fumikazu.kiyota@gmail.com
#

from handlers import BaseHandler, ValidationError
from models.user import User
import tornado.auth


class CommonHandler(BaseHandler):
    u"""ハンドラー共通クラス
    """
    def initialize(self):
        u"""ハンドラー共通初期設定
        """
        self.ERRORS = dict(
                email=False,
                login=False,
                dup_email=False,
                password=False,
                unknown=False
                )

    def render_setting(self, success=False):
        self.render("account/setting.html", errors=self.ERRORS,
                    success=success, me=self.me)

    def render_login(self, email=""):
        self.render("account/login.html", errors=self.ERRORS, email=email)

    def render_signup(self):
        self.render("account/signup.html", errors=self.ERRORS)

    def render_password(self, email=""):
        self.render("account/password.html", errors=self.ERRORS, email=email)

    def render_complete(self):
        self.render("account/complete.html")


class SettingHandler(CommonHandler):
    u"""アカウントの設定
    """
    @tornado.web.authenticated
    def get(self):
        self.render_setting()

    @tornado.web.authenticated
    def post(self):
        try:
            email = self.get_argument("email")
            password = self.get_argument("password", '')
            first_name = self.get_argument("first_name", '')
            last_name = self.get_argument("last_name", '')

            if self.me.email == email:
                email = self.me.email

            else:
                if User.objects.get(email=email):
                    raise ValidationError('dup_email')

            self.me.email = email

            if password:
                if len(password) < 6:
                    raise ValidationError('password')
                self.me.password = self.md5(password.encode('utf-8'))

            self.me.first_name = first_name
            self.me.last_name = last_name
            self.me.validate()

        except ValidationError as e:
            self.error()
            self.ERRORS[str(e)] = True
            self.render_setting()

        else:
            self.me.save()
            self.username(self.me.last_name, self.me.first_name)
            self.login(self.me.password, self.me.email)
            self.render_setting(success=True)


class LoginHandler(CommonHandler):
    u"""ログイン
    """
    def prepare(self):
        self.go_to_home()

    def get(self):
        self.render_login()

    def post(self):
        try:
            email = self.get_argument("email")
            password = self.get_argument("password")
            password = self.md5(password.encode('utf-8'))

            try:
                user = User.objects.get(email=email, password=password)
            except User.DoesNotExist:
                raise ValidationError('login')

        except ValidationError as e:
            self.error()
            self.ERRORS[str(e)] = True
            self.render_login(email=email)

        else:
            self.username(user.last_name, user.first_name)
            self.login(user.password, user.email)
            self.redirect("/home")


class LogoutHandler(BaseHandler):
    u"""ログアウト
    """
    def get(self):
        self.logout()


class SignupHandler(CommonHandler):
    u"""ユーザー登録
    """
    def prepare(self):
        self.go_to_home()

    def get(self):
        self.render_signup()

    def post(self):
        try:
            email = self.get_argument("email")
            try:
                user = User.objects.get(email=email)
            except User.DoesNotExist:
                raise ValidationError('dup_email')

            user = User()
            user.email = email
            user.validate()

        except ValidationError as e:
            self.error()
            self.ERRORS[str(e)] = True
            self.render_signup()

        else:
            self.render_password(email=email)


class PasswordHandler(CommonHandler):
    u"""パスワード設定
    """
    def prepare(self):
        self.go_to_home()

    def post(self):
        try:
            email = self.get_argument("email")
            password = self.get_argument("password")

            if len(password) < 6:
                raise ValidationError('password')
            password = self.md5(password.encode('utf-8'))

            user = User()
            user.email = email
            user.password = password
            user.validate()

        except ValidationError as e:
            self.ERRORS[str(e)] = True
            self.render_password(email=email)

        else:
            user.save()
            self.login(password, email)
            self.render_complete()
