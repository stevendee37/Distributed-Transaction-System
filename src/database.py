# This file provides the classes for creating and accessing databases
import os
import csv
from queue import Queue

class Database():
    def __init__(self, root_dir):
        self.queue = Queue()
        self.lock_taken = False # locking mechanism, True if held, False if available
        return