#!/usr/bin/env python
# -*- coding: utf-8 -*-

from boto import ec2 as boto_ec2
from time import sleep
from pymongo import MongoClient
import importlib
import sys
import argparse
import configparser
from datetime import datetime
from ast import literal_eval
import re

TAG_NAME_PREFIX = 'pub-proxy'


def db_update_proxy(db, proxy):
    cond = {'http': proxy['http']}
    sets = {'$set': proxy}
    getattr(db, 'proxy').update(cond, sets, upsert=True)
    for x in getattr(db, 'proxy').find({}):
        print x['http']


def add_proxy(ec2, tag_name, db):
    print 'adding proxy... %s' % tag_name
    req = ec2.request_spot_instances(price=price,
                                     instance_type=instance_type,
                                     image_id=image_id,
                                     availability_zone_group=availability_zone_group,
                                     key_name=key_name,
                                     security_group_ids=security_group_ids)
    job_instance_id = None
    job_sir_id = None
    sys.stdout.write('Please wait')
    sys.stdout.flush()
    while not job_instance_id:
        job_sir_id = req[0].id
        reqs = ec2.get_all_spot_instance_requests()
        for sir in reqs:
            if sir.id == job_sir_id:
                job_instance_id = sir.instance_id
                break
            sys.stdout.write('.')
            sys.stdout.flush()
            sleep(1)
    ec2.create_tags([job_instance_id], {'Name': tag_name})
    ec2.cancel_spot_instance_requests([job_sir_id])
    print 'done!'
    update_proxy(ec2, db)


def list_proxy(ec2, db):
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if re.match(TAG_NAME_PREFIX, x.tags.get('Name')):
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
                    print x.tags.get('Name'), x.id, x._state
    if not instance_ids:
        print 'not found...'
    else:
        update_proxy(ec2, db)


def delete_proxy(ec2, tag_name, db):
    print 'deleting proxy... %s' % tag_name
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if x.tags.get('Name') == tag_name:
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
                    print tag_name, x.id, x._state
    ec2.terminate_instances(instance_ids)
    update_proxy(ec2, db)


def update_proxy(ec2, db):
    print 'updating proxy data...'
    all_instances = ec2.get_all_instances()
    for inst in all_instances:
        for x in inst.instances:
            if re.match(TAG_NAME_PREFIX, x.tags.get('Name')):
                if 'running' in str(x._state):
                    url = x.public_dns_name
                    print x.id, x._state, url
                    proxy = {
                        'http': 'http://%s:8080' % url
                    }
                    db_update_proxy(db, proxy)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Spider Proxy Tools')
        parser.add_argument('-c', '--config',
                            action="store", dest="c",
                            default='.aws/config',
                            required=False,
                            help="aws config")
        parser.add_argument('-a', '--add-proxy', action="store_true",
                            dest="add_proxy", default=False,
                            help="add new proxy")
        parser.add_argument('--delete-proxy', action="store",
                            default=None, required=False,
                            help="proxy tag name")
        parser.add_argument('-H', '--host', action="store", dest="h",
                            default='localhost', required=False,
                            help="mongodb host")
        parser.add_argument('-p', '--port', action="store", dest="p",
                            default=27017, required=False,
                            help="mongodb port")
        parser.add_argument('-d', '--db', action="store", dest="db",
                            default='proxydb', required=False,
                            help="mongodb db")
        args = parser.parse_args()
        config = configparser.ConfigParser()
        config.read(args.c)
        aws_access_key_id = config.get('default', 'aws_access_key_id')
        aws_secret_access_key = config.get('default', 'aws_secret_access_key')
        region_name = config.get('default', 'region_name')
        price = config.get('default', 'price')
        instance_type = config.get('default', 'instance_type')
        image_id = config.get('default', 'image_id')
        key_name = config.get('default', 'key_name')
        availability_zone_group = literal_eval(
            config.get('default', 'availability_zone_group')
        )
        security_group_ids = literal_eval(
            config.get('default', 'security_group_ids')
        )
        tag_name = '%s-%s' % (TAG_NAME_PREFIX,
                              datetime.utcnow().strftime('%Y%m%d%H%I%S'))
        db = {
            'HOST': args.h,
            'PORT': int(args.p),
            'DB': args.db
        }
        mongo = MongoClient(args.h, int(args.p))
        db = mongo[args.db]
        ec2 = boto_ec2.connect_to_region(region_name=region_name,
                                         aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key)
        if args.delete_proxy:
            delete_proxy(ec2, args.delete_proxy, db)

        elif args.add_proxy:
            add_proxy(ec2, tag_name, db)

        else:
            list_proxy(ec2, db)

    except:
        print 'Please create .aws/config and add these configs'
        print '$ mkdir .aws'
        print '$ cat <<EOF > .aws/config'
        print 'aws_access_key_id = <aws_access_key_id>'
        print 'aws_secret_access_key = <aws_secret_access_key>'
        print 'region_name = ap-northeast-1'
        print 'price = 0.5'
        print 'image_id = ami-13527d7d'
        print 'instance_type = m3.medium'
        print 'availability_zone_group = ["ap-northeast-1b"]'
        print 'key_name = proxy'
        print 'security_group_ids = ["sg-22d52b22"]'
        print 'EOF'
        importlib.import_module('mylib.logger').sentry()
