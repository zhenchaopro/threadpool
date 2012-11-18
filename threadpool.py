#!/usr/bin/env python

"""Object-oriented thread pool framework.

A thread pool is an object that maintains a pool of worker threads to perform
time consuming operations in parallel. It assigns jobs to the threads
by putting them in a work request queue, where they are picked up by the
next available thread. This then performs the requested operation in the
background and puts the results in the same queue.

The most import thing is, all worker threads are producer and consumer, since
them all get task from the queue and then push all its result into the same queue.

"""

import httplib
import urllib2
import posixpath
import urlparse
import Queue
import threading
import logging
from BeautifulSoup import BeautifulSoup, SoupStrainer

__all__ = ['EnhanceQueue', 'Worker', 'ThreadPool']
__version__ = 'v1.0'
__author__ = 'Emrys'

class EnhanceQueue(Queue.Queue):
    """
    A more flexible Queue for Threadpool.

    Implement a new method to ask whether all items in queue
    were already processed.
    """

    def __init__(self):
        Queue.Queue.__init__(self)

    def all_done(self):
        """
        Returns true if all tasks have been done, or false otherwise.
        """
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
    """Worker thread."""

    def __init__(self, work_queue, condition, timeout=0):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.condition = condition
        self.timeout = timeout
        self.setDaemon(True)
        self.start()

    def run(self):
        """Main loop of get-workrequest, do-work stuffs."""

        while True:
            try:
                work_request = self.work_queue.get(timeout=self.timeout)
                logging.debug("get a url from queue %s", work_request)

                # Do work and append work_results into work_queue
                work_results = self.do_work(work_request);
                for result_item in work_results:
                    self.work_queue.put(result_item)
                self.work_queue.task_done()
                logging.debug("work_queue.size: %d", self.work_queue.qsize())

                # notify other workers when there are task available
                if not work_results:
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

    def do_work(self, work_request):
        """docstring for do_work"""
        return work_request.callable(work_request.args)

class ThreadPool():
    """
    A thread pool with some initialized work requests.

    See the module docstring for more information.
    """

    def __init__(self, workerNum, init_work_request):
        self.work_queue = EnhanceQueue()
        self.condition = threading.Condition()
        self.add_task(init_work_request)
        self.workers = []
        self.add_workers(workerNum)

    def add_workers(self, workerNum):
        for i in range(workerNum):
            worker = Worker(self.work_queue, self.condition)
            self.workers.append(worker)
        logging.info("Add %d worker to threadpool.", workerNum)

    def poll(self):
        """Wait for all the tasks to be done."""

        logging.info("Wait for all task to be done...")
        self.work_queue.join()
        logging.info("All tasks have been done.")

        # When the last worker exits, other threads are still waiting
        # to be notify. Notify all of them.
        logging.info("Nofify all the workers to exit...")
        self.condition.acquire()
        self.condition.notifyAll()
        self.condition.release()

        for worker in self.workers:
            worker.join()
        self.workers = []
        logging.info("All workers exit successfully, Threadpool now exit...")

    def add_task(self, init_work_request):
        self.work_queue.put(init_work_request)

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

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s --%(threadName)s-- %(message)s',
            level=logging.DEBUG)

    def spider(*args):
        logging.debug("downloading %s", url)
        links = []
        response = downloadURL(url)

        if depth == 0:
            return links
        # parseHTMLLinks
        logging.debug("parsing links...")
        links = parseHTMLLinks(response, url)
        return links

    class WorkRequest:
        def __init__(self, callable, *args):
            self.callable = callable
            self.args = args or []
            pass

        def __str__(self):
            return "WorkRequest id=%s args=%s" % (id(self), self.args)

    try:

        logging.info("Start....")
        url = "http://otl.sourceforge.net/"
        depth = 1;
        work_request = WorkRequest(spider, url, depth)
        pool = ThreadPool(10, work_request)
        pool.poll()
        logging.info("End!!")
    except KeyboardInterrupt, e:
        print "KeyboardInterrupt!!"
