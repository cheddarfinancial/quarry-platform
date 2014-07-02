# Standard Library
import time
from threading import Lock

# Third Party

# Local


class LRU():

    def __init__(self, maxSize=1000):
        self.maxSize = maxSize
        self.lru = dict()
        self.items = dict()
        self.lock = Lock()

    def add(self, id, item):

        self.lock.acquire()

        try:

            if len(self.lru) > self.maxSize:
                remKey = sorted(self.lru.items(), key=lambda x: x[1], reverse=True)[0][0]
                del self.lru[remKey]
                del self.items[remKey]

            self.lru[id] = time.time()
            self.items[id] = item

        finally:

            self.lock.release()

    def get(self, id):

        self.lock.acquire()

        item = None
        try:

            item = self.items.get(id,None)
            if item is not None:
                self.lru[id] = time.time()
        
        finally:

            self.lock.release()

        return item

    def remove(self, id):

        self.lock.acquire()

        try:

            del self.lru[id]
            del self.items[id]

        finally:

            self.lock.release()
