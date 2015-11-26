#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import datetime
from mongoengine import connect
from mongoengine import Document
from mongoengine import StringField
from mongoengine import EmailField
from mongoengine import DateTimeField
from mongoengine import ValidationError
from mongoengine.connection import get_db, get_connection

from models.user import *
import os

class TestMongoEngine(unittest.TestCase):

    def setUp(self):
        # データベースに接続
        addr = '127.0.0.1'
        port = 27017
        connect('test', host=addr, port=port)
        self.conn = get_connection()
        self.db = get_db()

    def tearDown(self):
        # コレクションの削除
        User.drop_collection()

    def test_create(self):
        """
        create テスト
        """
        user = User(email='hogehoge@example.com', first_name='hoge', last_name='foo')
        user.save()

        u_obj = User.objects.first()
        u_obj.first_name = "change"
        u_obj.save()

        self.assertEqual(user.first_name, "hoge")

        user.reload()
        self.assertEqual(user.first_name, "change")

    def test_validation(self):
        """
        validation テスト
        """
        user = User()
        self.assertRaises(ValidationError, user.validate)

        user.email = 'valid@example.com'
        user.validate()

        user.first_name = 10
        self.assertRaises(ValidationError, user.validate)

    def test_read(self):
        user = User(email='hogehoge@example.com', first_name='hoge', last_name='foo')
        user.save()

        collection = self.db[User._get_collection_name()]
        u_obj = collection.find_one({'email': 'hogehoge@example.com'})
        self.assertEqual(u_obj['email'], 'hogehoge@example.com')
        self.assertEqual(u_obj['first_name'], 'hoge')
        self.assertEqual(u_obj['last_name'], 'foo')
        self.assertEqual(u_obj['_id'], user.id)

        u_err = User(email='root@localhost')
        self.assertRaises(ValidationError, u_err.save)
        try:
            u_err.save(validate=False)
        except ValidationError:
            self.fail()

if __name__ == '__main__':
    unittest.main()
