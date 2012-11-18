#!/usr/bin/env python

import httplib
import urllib2
import posixpath
import urlparse
import Queue
import threading
import logging
from BeautifulSoup import BeautifulSoup, SoupStrainer

__metaclass__ = type
# common file extensions that are not followed if they occur in links
ALLOW_EXTENTIONS= ['php','html','htm','xml','xhtml','xht','xhtm',
        'asp','aspx','msp','mspx','php3','php4','php5','txt','shtm',
        'shtml','phtm','phtml','jhtml','pl','jsp','cfm','cfml','do','py'
        ]

def withAllowedExtention(url):
    directory, filename = posixpath.splitext(urlparse.urlparse(url).path)
    if not filename:
        return False
    return filename[1:].lower() in ALLOW_EXTENTIONS

user_agent = "Mozilla/5.0 (X11; Linux i386; rv:16.0) Gecko/20100101 Firefox/16.0.1"
referer = "http://www.baidu.com/"
host = ""   # This options is needed when encountering a virtual machine.
headers = {'User-Agent': user_agent, 'Referer': referer}

def downloadURL(url):
    req = urllib2.Request(url, None, headers)
    try:
        response = urllib2.urlopen(req)
    except urllib2.HTTPError, e:
        logging.error("The server could not fullfill the request.")
        logging.error("Error code: %d", e.code);
        return
    except urllib2.URLError, e:
        logging.error("We failed to reach the server.")
        logging.error("Reason: %s", e.reason)
        return
    except httplib.BadStatusLine,e:
        logging.critical("Error: server responds with a HTTP status code that we don’t understand:  %s", url)
        return
    except IOError, e:
        logging.error(e)
        return
    except UnicodeEncodeError, e:
        logging.error(e)
        return

    return response

def htmldecode(s):
    """Unescaping the HTML special characters"""
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    s = s.replace("&quot;", "\"")
    s = s.replace("&apos;","'")
    s = s.replace("&amp;", "&")
    return s


def prettify(href, currentURL):
    """ return a standard url.
    """
    joined_href = urlparse.urljoin(currentURL, href)
    if joined_href.startswith('javascript'):
        return
    if not withAllowedExtention(joined_href):
        return
    if "http://" in joined_href or "https://" in joined_href:
        if "#" in joined_href:
            return htmldecode(joined_href.split('#')[0])
        return htmldecode(joined_href)


def parseHTMLLinks(htmlContent, currentURL):
    """ Parse the HTML/XHTML content to get links.
    """
    linkAnchors = []
    if not htmlContent:
        return linkAnchors
    linkSoupStrainer = SoupStrainer('a')
    tags = BeautifulSoup(htmlContent, parseOnlyThese=linkSoupStrainer)
    for tag in tags:
        if 'href' in dict(tag.attrs):
            pretty_link = prettify(tag['href'], currentURL)
            if pretty_link:
                linkAnchors.append(pretty_link)
    return linkAnchors

class EnhanceQueue(Queue.Queue):
    """A more flexible Queue for threadpool."""
    def __init__(self):
        Queue.Queue.__init__(self)

    def all_done(self):
        done = False
        self.all_tasks_done.acquire();
        try:
            if self.unfinished_tasks:
                done = False
            else:
                done = True
        finally:
            self.all_tasks_done.release()

        return done

class Worker(threading.Thread):
    """docstring for Worker"""
    def __init__(self, work_queue, condition, timeout=0):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.condition = condition
        self.timeout = timeout
        self.setDaemon(True)
        self.start()

    def run(self):
        """main loop"""
        while True:
            try:
                url, depth = self.work_queue.get(timeout=self.timeout)
                logging.debug("get a url from queue %s:%d", url, depth)
                links = self.do_work(url, depth);
                for link in links:
                    self.work_queue.put((link, depth-1))
                logging.debug("work_queue.size: %d", self.work_queue.qsize())
                self.work_queue.task_done()
                # notify other workers when there are task available
                if not links:
                    self.condition.acquire()
                    self.condition.notifyAll()
                    self.condition.release()
            except Queue.Empty:
                if self.work_queue.all_done():
                    logging.debug("All task has been done, breaking...")
                    break
                else:
                    logging.debug("Queue is empty, but other threads are still in procedure.wait..")
                    self.condition.acquire()
                    self.condition.wait()
                    self.condition.release()
                    continue

    def do_work(self, url, depth):
        """docstring for do_work"""
        # download page
        logging.debug("downloading %s", url)
        response = downloadURL(url)

        links = []
        if depth == 0:
            return links
        # parseHTMLLinks
        logging.debug("parsing links...")
        links = parseHTMLLinks(response, url)
        return links

class ThreadPool():
    """docstring for ThreadPool"""
    def __init__(self, workerNum, url, depth):
        self.work_queue = EnhanceQueue()
        self.condition = threading.Condition()
        self.add_task(url, depth)
        self.workers = []
        self._add_workers(workerNum)

    def _add_workers(self, workerNum):
        for i in range(workerNum):
            worker = Worker(self.work_queue, self.condition)
            self.workers.append(worker)
        logging.info("add 10 worker to threadpool.")

    def poll(self):
        """wait for all the task to be done."""
        logging.info("wait for all task to be done...")
        self.work_queue.join()

        self.condition.acquire()
        self.condition.notifyAll()
        self.condition.release()

        for worker in self.workers:
            worker.join()
        self.workers = []

        logging.info("Threadpool now exit...")


    def add_task(self, url, depth):
        self.work_queue.put((url, depth))

if __name__ == '__main__':
    #url = "http://otl.sourceforge.net/"
    url = "http://www.sina.com.cn/"
    depth = 2
    try:
        logging.basicConfig(filename='spider.log', format='%(asctime)s --%(threadName)s-- %(message)s',
                level=logging.DEBUG)

        logging.info("Start....")
        pool = ThreadPool(10, url, depth)
        pool.poll()
        logging.info("End!!")
    except Exception, e:
        logging.error(e)
