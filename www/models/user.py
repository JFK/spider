#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from mongoengine import connect
from mongoengine import Document
from mongoengine import StringField
from mongoengine import EmailField
from mongoengine import IntField
from mongoengine import DateTimeField
from mongoengine import ValidationError

USER_STAT_BLOCKED = -3
USER_STAT_CLOSED = -2
USER_STAT_SUSPEND = -1
USER_STAT_GUEST = 0
USER_STAT_MEMBER = 1

USER_ROLE_BASIC = 1
USER_ROLE_ADMIN = 10

USER_PLAN_BASIC = 10
USER_PLAN_UNLIMITED = 100

class User(Document):
    email = EmailField(required=True, unique=True)
    first_name = StringField(max_length=64)
    last_name = StringField(max_length=64)
    password = StringField(max_length=32)
    role = IntField(default=USER_ROLE_BASIC, min_value=USER_ROLE_BASIC, max_value=USER_ROLE_ADMIN)
    state = IntField(default=USER_STAT_GUEST, min_value=USER_STAT_BLOCKED, max_value=USER_STAT_MEMBER)
    plan = IntField(default=USER_PLAN_BASIC, min_value=USER_PLAN_BASIC, max_value=USER_PLAN_UNLIMITED)
    date_modified = DateTimeField(default=datetime.datetime.utcnow, required=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow, required=True)
