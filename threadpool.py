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

import Queue
import threading
import logging

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
        return work_request.callable(work_request.url)

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
