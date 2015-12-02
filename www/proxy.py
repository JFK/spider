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
import random
import re

TAG_NAME_PREFIX = 'pub-proxy'
mongo = MongoClient()
db = mongo['proxydb']

config = configparser.ConfigParser()
config.read('.aws/config')
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
ec2 = boto_ec2.connect_to_region(region_name=region_name,
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)


def select_proxy():
    return random.choice(map(lambda doc: doc, db.proxy.find({})))


def db_update_proxy(proxy):
    cond = {'http': proxy['http']}
    sets = {'$set': proxy}
    getattr(db, 'proxy').update(cond, sets, upsert=True)
    for x in getattr(db, 'proxy').find({}):
        print x['http']


def add_proxy():
    tag_name = create_tag_name()
    print 'adding proxy... %s' % tag_name
    req = ec2.request_spot_instances(price=price,
                                     instance_type=instance_type,
                                     image_id=image_id,
                                     availability_zone_group=availability_zone_group,
                                     key_name=key_name,
                                     security_group_ids=security_group_ids)
    job_instance_id = None
    job_sir_id = None
    sys.stdout.write('please wait')
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
    update_proxy()


def list_proxy():
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if x.tags.get('Name') and re.match(TAG_NAME_PREFIX, x.tags.get('Name')):
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
                    print x.tags.get('Name'), x.id, x._state
    if not instance_ids:
        print 'not found...'
    else:
        update_proxy()


def create_tag_name():
    return '%s-%s' % (TAG_NAME_PREFIX,
                      datetime.utcnow().strftime('%Y%m%d%H%I%S'))


def shutdown_proxy():
    print 'shutting down proxy...'
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if re.match(TAG_NAME_PREFIX, x.tags.get('Name')):
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
    if not instance_ids:
        print 'not found any proxy...'
    else:
        print "Are you sure to terminate the instances bellow: [Y/n]"
        print 'Instances... %s' % ", ".join(instance_ids)

        YorN = sys.stdin.readline().strip()
        if not YorN or 'y' == YorN.lower():
            sys.stdout.write('Terminating....')
            sys.stdout.flush()
            ec2.terminate_instances(instance_ids)
            getattr(db, 'proxy').remove({})
        else:
            print 'Aborting...'

def reload_proxy():
    print 'reloadng proxy...'
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if re.match(TAG_NAME_PREFIX, x.tags.get('Name')):
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
    if not instance_ids:
        print 'not found any proxy...'
    else:
        for x in range(0, len(instance_ids)):
            add_proxy()
        print 'terminating instances... %s' % ", ".join(instance_ids)
        ec2.terminate_instances(instance_ids)
        getattr(db, 'proxy').remove({})
        update_proxy()


def delete_proxy(tag_name):
    print 'deleting proxy... %s' % tag_name
    all_instances = ec2.get_all_instances()
    instance_ids = []
    for inst in all_instances:
        for x in inst.instances:
            if x.tags.get('Name') == tag_name:
                if 'running' in str(x._state):
                    instance_ids.append(x.id)
                    print tag_name, x.id, x._state
    if not instance_ids:
        print 'not found any proxy...'
    else:
        ec2.terminate_instances(instance_ids)
        getattr(db, 'proxy').remove({})
        update_proxy()


def update_proxy():
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
                    db_update_proxy(proxy)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Spider Proxy Tools')
        parser.add_argument('--reload', action="store_true",
                            dest="reload", default=False,
                            help="reload proxy")
        parser.add_argument('--shutdown', action="store_true",
                            dest="shutdown", default=False,
                            help="shutdown all proxies")
        parser.add_argument('--add', action="store",
                            dest="add", default=0, required=False,
                            help="add new proxy")
        parser.add_argument('--delete', action="store",
                            default=None, required=False,
                            help="delete proxy")
        args = parser.parse_args()
        if args.shutdown:
            shutdown_proxy()

        elif args.reload:
            reload_proxy()

        elif args.delete:
            delete_proxy(args.delete)

        elif args.add:
            for x in range(0, int(args.add)):
                add_proxy()

        else:
            list_proxy()

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
