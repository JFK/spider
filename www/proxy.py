#!/usr/bin/env python
# -*- coding: utf-8 -*-

from boto import ec2 as boto_ec2
from time import sleep
from pymongo import MongoClient
import importlib
import sys
import optparse
import configparser


config = configparser.ConfigParser()
config.read('.aws/config')

az = {
    'b': ['ap-northeast-1b'],
    'c': ['ap-northeast-1c']
}
subnet = {
    'b': 'subnet-4654242f',
    'c': 'subnet-11532378'
}
try:
    aws_access_key_id = config.get('default', 'aws_access_key_id')
    aws_secret_access_key = config.get('default', 'aws_secret_access_key')
    region_name = config.get('default', 'region_name')
    price = config.get('default', 'price')
    instance_type = config.get('default', 'instance_type')
    image_id = config.get('default', 'image_id')
    availability_zone_group = config.get('default', 'availability_zone_group')
    key_name = config.get('default', 'key_name')
    security_group_ids = config.get('default', 'security_group_ids')
    tag_name = config.get('default', 'tag_name')
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
    print 'tag_name = pub-proxy-0'
    print 'EOF'
    sys.exit(1)


def update_worker(db, pname, proxy):
    mongo = MongoClient(db['HOST'], db['PORT'])
    db = mongo[db['DB']]
    cond = {'pname': pname}
    sets = {'$set': {'proxy': proxy}}
    getattr(db, 'worker').update(cond, sets)


def add_proxy(ec2):
    req = ec2.request_spot_instances(price=price,
                                     instance_type=instance_type,
                                     image_id=image_id,
                                     availability_zone_group=availability_zone_group,
                                     key_name=key_name,
                                     security_group_ids=security_group_ids)
    job_instance_id = None
    job_sir_id = None
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


def update_proxy(ec2, pname, db):
    all_instances = ec2.get_all_instances()
    proxies = []
    for inst in all_instances:
        for x in inst.instances:
            if x.tags.get('Name') == tag_name:
                if 'running' in str(x._state):
                    url = x.public_dns_name
                    print x.id, x._state, url
                    proxy = {
                        'http:': 'http://%s:8080' % url
                    }
                    proxies.append(proxy)
    update_worker(db, pname, proxies)


def main(pname, db, add_proxy=False):
    ec2 = boto_ec2.connect_to_region(region_name=region_name,
                                     aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
    if add_proxy:
        add_proxy(ec2)
    update_proxy(ec2, pname, db)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-p', '--project-name',
                      action="store", dest="p",
                      help="project module name")
    parser.add_option('-a', '--add-proxy', action="store_true",
                      dest="add", default=False,
                      help="add new proxy")
    opts, args = parser.parse_args()
    m = importlib.import_module('projects.%s' % opts.p)
    db = getattr(m, 'MONGODB')
    main(opts.p, db, add_proxy=opts.add)
