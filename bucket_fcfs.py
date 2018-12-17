#!/usr/bin/env python3
import functools

import threading
import datetime
import time
import math
from collections import deque
from multiprocessing import dummy


def now():
    return str(datetime.datetime.now().time()).split('.')[0]


def log_call(foo):
    @functools.wraps(foo)
    def inner(_self, *args):
        print("Call", foo.__name__, "args:", args, now())
        try:
            return foo(_self, *args)
        finally:
            print("Exit", foo.__name__, "args:", args, now())
    return inner


class Bucket(object):
    def __init__(self, capacity, rate):
        self.capacity = capacity
        self.rate = rate

        self.volume_mutex = threading.Lock()
        self.volume = 0
        self.update_time = time.time()

        self.queue_mutex = threading.Lock()
        self.queue = deque()

    @log_call
    def get(self, volume):
        with self.queue_mutex:
            wait_object = threading.Event()
            # to make sure if anyone put back anything after _self_and_get = we will be alerted
            self.queue.append(wait_object)
            if len(self.queue) > 1:
                wait_time = None
            else:
                wait_time = self._check_and_get(volume)

            if wait_time is not None and wait_time == 0:
                self.queue.pop()
                return

        while True:
            wait_object.wait(wait_time)
            wait_object.clear()
            wait_time = self._check_and_get(volume)
            if wait_time == 0:
                with self.queue_mutex:
                    self.queue.popleft()
                    if self.queue:
                        self.queue[0].set()
                    return

    @log_call
    def put(self, volume):
        with self.volume_mutex:
            self._update_volume(volume)

        with self.queue_mutex:
            if self.queue:
                self.queue[0].set()

    def _check_and_get(self, volume):
        with self.volume_mutex:
            self._update_volume()
            if self.volume >= volume:
                self.volume -= volume
                return 0
            else:
                return math.ceil((volume - self.volume) / self.rate)

    def _put_volume(self, volume):
        with self.volume_mutex:
            self._update_volume(volume)

    def _update_volume(self, extra=0):
        secs_elapsed = math.floor(time.time() - self.update_time)
        self.update_time += secs_elapsed
        self.volume = min(self.rate * secs_elapsed + extra + self.volume, self.capacity)


def main():
    def t(args):
        delay, func, volume = args
        time.sleep(delay)
        func(volume)

    b = Bucket(100, 2)
    data = [
        (0, b.get, 10),
        (1, b.get, 5),
        (2, b.put, 3),
    ]

    with dummy.Pool(processes=10) as p:
        p.map(t, data)


if __name__ == "__main__":
    main()
