#!/usr/bin/env python

import urllib2
import re
import logging
import unittest
import traceback
from datetime import datetime

from threadpool import ThreadPool

logging.basicConfig(format='%(asctime)s --%(threadName)s-- %(message)s',
        level=logging.DEBUG)

headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:13.0) Gecko/20100101 Firefox/13.0.1',
            'Cookie':'''__utma=268877164.2104051766.1342944471.1342944471.1342944471.1;
                        __utmb=268877164; __utmc=268877164; __utmz=268877164.1342944471.1.1.utmccn=(direct)|utmcsr=(direct)|utmcmd=(none);
                        AJSTAT_ok_pages=32; AJSTAT_ok_times=1; acaspvid=64186014361410816;
                        __gads=ID=df2c2ffd219dfd9f:T=1342944471:S=ALNI_MaT1tsjt9qJSxSKrvyPHLdvQBVmZA; acs=1'''
         }

def spider(url):
    try:
        req = urllib2.Request(url, None, headers)
        f = urllib2.urlopen(req)
        data = f.read()
        pat_imgs = re.compile(u"<img.*src=\"([^\"]*)", re.I | re.U)
        imgs = list(set(pat_imgs.findall(data)))
        new_items = []
        for i in imgs:
            if i != "":
                item = WorkRequest(spider, i)
                new_items.append(item)
        return new_items
    except:
        traceback.print_exc()
        return []


class WorkRequest:
    def __init__(self, callable, url):
        self.callable = callable
        self.url = url

    def __str__(self):
        return "WorkRequest id=%s url=%s" % (id(self), self.url)

class TestThreadpool(unittest.TestCase):
    def testImageDownload(self):
        logging.debug('Start at %s', datetime.now())
        url = 'http://f1.163.com'
        # url = 'http://news.sina.com.cn/photo/'
        work_request = WorkRequest(spider, url)
        pool = ThreadPool(10, work_request)
        pool.poll()

if __name__ == '__main__':
    unittest.main()
