from lstore.index import Index
from lstore.page import Page
from lstore.Bufferpool import BufferPool
from lstore.lock import Lock, LockManager
from time import time
import struct
import os
import pickle
import threading

from timeit import default_timer as timer
from decimal import Decimal

#last 4 columns of all records are listed below:
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table: 

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, path='none', bufferpool='none', load='none'):
        self.name = name
        self.key = key
        self.num_columns = num_columns #excludes the 4 columns written above
        self.max_records = 64 #the max_records able to be stored in one page, this MUST mirror max_records from page class
        self.lock_manager = LockManager()
        self.thread_lock = threading.Lock()
        self.update_thread_lock = threading.Lock()
        self.merge_thread_lock = threading.Lock()
        self.records_updating = []
        
        #organize folders related to this table
        self.path = path
        if path == 'none':
            self.path = "./"+name
            os.mkdir(self.path)
        directory = "base_pages"
        self.base_path = os.path.join(self.path, directory)
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path) 
        directory = "tail_pages"
        self.tail_path = os.path.join(self.path, directory)
        if not os.path.exists(self.tail_path):
            os.makedirs(self.tail_path)

        self.bufferpool = bufferpool
        if bufferpool == 'none':
            self.bufferpool = BufferPool()
        self.bufferpool.add_table(self.name, self)

        self.page_directory = {}
        self.tail_page_directory = {}
        self.num_pages = -1 #stores the amount of pages minus 1
        self.num_tail_pages = -1 #stores the amount of pages minus 1
        if load == 'none':
            self.init_page_dir()
            self.init_tail_page_dir()

        self.rid = 0  #rid of the next spot in the page range (not of the latest record)
        self.index = Index(self)
        for i in range(self.num_columns):
            self.index.create_index(i)
        self.total_tail_records = 0
        self.tps = 0 # INDEX FIX: Should be an array that represents each column
        pass

    def init_page_dir(self): #adds one set of physical pages to the page_directory, in case the base pages have filled up or to initialize the page directory
        for i in range(self.num_columns+4):
            self.num_pages += 1
            page = Page()
            self.bufferpool.initPages(self.name, page, self.num_pages, True)
        pass

    def init_tail_page_dir(self): #adds one set of physical pages to the tail_page_directory, in case the tail pages have filled up or to initialize the tail page directory
        for i in range(self.num_columns+5):
            self.num_tail_pages += 1
            page = Page()
            self.bufferpool.initPages(self.name, page, self.num_tail_pages, False)
        pass

    def __merge(self, current_tail_record):
        # print("merge is happening...") <-- if uncommented, this will print even on the first ever update
        # tail_records = self.tail_page_directory.copy() # BUFFERPOOL FIX: obtain copies from disk of all tail records
        ...
        while True:
            success = True
            for record in self.records_updating:
                if current_tail_record > record:
                    success = False
            if success:
                break

        start = timer()
        #delete timer stuf ------------------------------------------------------
        tail_time = timer()
        tail_records = self.bufferpool.get_tail_pages(self.name)
        end = timer()
        # print("   time to get tail pages : ", Decimal(end - tail_time).quantize(Decimal('0.01')), "seconds")
        #delete some testing stuf above ---------------------------------------------
        base_page_copies = {}
        updatedQueue = set()
        max_records = self.max_records
        for i in reversed(range(self.total_tail_records - self.tps)): #ex. if there are 40 tail records (latest rid=39) and tps at rid=27 (tail-record with rid=27 and beyond still need to be merged), then creates a range from 12 to 0
            tail_rid = i + self.tps
            tail_page_index = (tail_rid // max_records)*(self.num_columns + 5) #tail page now has a 5th column, base-rid
            base_rid = tail_records[tail_page_index + 4 + self.num_columns].read_val(tail_rid)
            base_page_index = (base_rid // max_records)*(self.num_columns + 4)

            #print("base id", base_rid)
            if base_rid not in updatedQueue: # skips merge if base page rid is already in updated_queue
                #print("merged")
                for i in range(self.num_columns): # replaces values of base record with latest tail record
                    value = tail_records[tail_page_index + 4 + i].read_val(tail_rid)
                    base_page = None
                    if (base_page_index + 4 + i) in base_page_copies: # if page has been stored, retrieve it from memory
                        base_page = base_page_copies[base_page_index + 4 + i]
                    else: # else retrieve it from disk
                        # base_page = self.page_directory[base_page_index + 4 + i].copy() #BUFFERPOOL FIX: obtain copy from disk 
                        base_page = self.bufferpool.get_page_copy(self.name, base_page_index + 4 + i).copy()
                        base_page_copies[base_page_index + 4 + i] = base_page
                    base_page.overwrite(base_rid, value)

                # in place updated for metadata
                if (base_page_index + 3) in base_page_copies: # if page has been stored, retrieve it from memory
                    base_page_copies[base_page_index + 3].overwrite(base_rid, 0)
                else: # else retrieve it from disk
                    base_page = self.bufferpool.get_page_copy(self.name, base_page_index + 3).copy()
                    # base_page = self.page_directory[base_page_index + 3].copy() #BUFFERPOOL FIX: obtain copy from disk 
                    base_page_copies[base_page_index + 3] = base_page
                    base_page.overwrite(base_rid, 0)
            updatedQueue.add(base_rid)
        for page_num in base_page_copies:
            self.bufferpool.replace_page(self.name, page_num, base_page_copies[page_num])
            # self.page_directory[page_num] = base_page_copies[page_num] #BUFFERPOOL FIX: push updated pages back into disk and bufferpool
        end = timer()
        # print(" merging should be done Total time Taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
        self.tps = self.total_tail_records

    def close(self):
        filename = "tabledata.pickle"
        path = os.path.join(self.path, filename)
        self.index.thread_lock = None
        self.index.createIndex_thread_lock = None
        with open(path, 'wb') as f:
            pickle.dump(self, f) #dump all metadata, pagedirectory, and index 
    
    def open(self):
        filename = "tabledata.pickle"
        path = os.path.join(self.path, filename)
        with open(path, 'rb') as f:
            loaded_table = pickle.load(f)

        # directly update the current instance's attributes
        self.__dict__.update(loaded_table.__dict__)
        # Re-bind bufferpool's reference to this table
        self.bufferpool.add_table(self.name, self)
        self.index.thread_lock = threading.Lock()
        self.index.createIndex_thread_lock = threading.Lock()        
