# spider

Very Simple Web Crawler Framework

## What you need

* python2.7
* MongoDB
* Redis
* rq

## Vagrant up or ansible playbook

Visit https://github.com/JFK/spider-vagrant
Run ansible to setup spider server or vagrant up.

## How to use

You can create own project to crawl a web page.
Very simple, just add a new module to projects dir.
See sample project `projects/sample.py`.

## How to run

After your create a new module, do this.

```
$ python spider -p sample
$ rqwoker -vvv normal
$ rqinfo normal -i 1
```

## Using forwarding proxy

ngnix configuration sample

```
server {
    listen 8080;
    location / {
        resolver 8.8.8.8;
        proxy_pass http://$http_host$uri$is_args$args;
    }
}
```

