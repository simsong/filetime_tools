# https://medium.com/@shashwat_ds/a-tiny-multi-threaded-job-queue-in-30-lines-of-python-a344c3f3f7f0
from threading import Thread
import Queue 
import time

class TaskQueue(Queue.Queue):

    def __init__(self, num_workers=1):
        Queue.Queue.__init__(self)
        self.num_workers = num_workers
        self.start_workers()

    def add_task(self, task, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def start_workers(self):
        for i in range(self.num_workers):
            t = Thread(target=self.worker)
            t.daemon = True
            t.start()

    def worker(self):
        while True:
            tupl = self.get()
            item, args, kwargs = self.get()
            item(*args, **kwargs)  
            self.task_done()


def tests():
    def blokkah(*args, **kwargs):
        time.sleep(5)
        print "Blokkah mofo!"

    q = TaskQueue(num_workers=5)

    for item in range(10):
        q.add_task(blokkah)

    q.join()       # block until all tasks are done
    print "All done!"

if __name__ == "__main__":
tests()
