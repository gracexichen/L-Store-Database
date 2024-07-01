from lstore.table import SCHEMA_ENCODING_COLUMN, Table, Record
from lstore.index import Index
import struct
RID_COLUMN = 1
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
class Query:
    def __init__(self, table):
        self.table = table

    def delete(self, primary_key):
        # Locate the RID for the primary key
        result = self.table.index.locate(self.table.key, primary_key)
        if not result:
            return False  # if primary key is not found
        rid = result[0]

        # Calculate the page and record number
        max_records = self.table.max_records
        page_number = rid // max_records
        record_number = rid % max_records

        # Determine the base page index for the schema encoding column
        schema_encoding_page_col = 2

        # Retrieve the base page for the schema encoding column
        base_page = self.table.bufferpool.get_page(self.table.name, page_number * (self.table.num_columns + 4) + schema_encoding_page_col, True)

        # Mark the record as deleted by setting its schema encoding to -1
        base_page.overwrite(record_number, -1)

        key_col = self.table.key
        rid = self.table.index.locate(key_col, primary_key)[0]
        record = self.select_record(primary_key, key_col, [1]*self.table.num_columns)[0]
        for i in range(len(record)):
            self.table.index.delete_index(i, record[i], rid)

        return True
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, t_id=None):
        primary_key = columns[self.table.key]
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid == []:
            rid = 0
            num_pages = 0
            with self.table.thread_lock:
                rid = self.table.rid
                self.table.rid += 1
                self.table.lock_manager.acquire_exclusive_lock(rid, t_id)
                if (rid != 0 and rid % self.table.max_records == 0): #if there's no capacity
                    self.table.init_page_dir() #add one base page (a set of physical pages, one for each column)
            self.table.insert_column(columns, -1, rid, 0, 0)
            for i in range(self.table.num_columns):
                self.table.index.add_index(i, columns[i], rid)
            return True
        else:
            return False

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # get index with search_key
        # go to base page of record
        # check indirection column (whether it has been updated or not)
        # if it has been updated, go to tail page and find the record
        # if it has not been updated, retrieve the projected_columns 
        # create a record with the info and return it
        record_list = []
        key_rid = self.table.index.locate(search_key_index, search_key)
        for key in key_rid:
            max_records = self.table.max_records #64 records
            base_page_index = (key // max_records)*(self.table.num_columns+4)
            indirection = self.table.bufferpool.get_page(self.table.name, base_page_index, True).read_val(key) # other version: change to only base_page_index
            columns = []
            if indirection == -1 or indirection < self.table.tps: # has not been updated (return record in base page)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.table.bufferpool.get_page(self.table.name, base_page_index+i+4, True).read_val(key)
                        columns.append(data)
            else: # has been updated, get tail page (return record in tail page)
                tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.table.bufferpool.get_page(self.table.name, tail_page_index+i+4, False).read_val(indirection)
                        columns.append(data)
            new_record = Record(key, search_key, columns)
            record_list.append(new_record)
        return record_list
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        record_list = []
        key_rid = self.table.index.locate(search_key_index, search_key)
        for key in key_rid:
            max_records = self.table.max_records #64 records
            base_page_index = (key // max_records)*(self.table.num_columns+4)
            indirection = self.table.bufferpool.get_page(self.table.name, base_page_index, True).read_val(key) # other version: change to only base_page_index
            columns = []
            if indirection == -1 or indirection < self.table.tps: # has not been updated (return record in base page)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.table.bufferpool.get_page(self.table.name, base_page_index+i+4, True).read_val(key)
                        columns.append(data)
            else: # has been updated, get tail page (return record in tail page with correct version)
                tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                counter = -relative_version # how many times we have to go back
                has_past = True # if there is more versions before the current tail record
                while(counter > 0 and has_past): # keep going back until it reaches the desired version
                    tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                    indirection = self.table.bufferpool.get_page(self.table.name, tail_page_index, False).read_val(indirection)
                    counter -= 1
                    if indirection == -1 or indirection < self.table.tps:
                        has_past = False
                if has_past:
                    tpage_set = indirection // max_records
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1:
                            data = self.table.bufferpool.get_page(self.table.name, i+4+tpage_set*(self.table.num_columns+5), False).read_val(indirection)
                            columns.append(data)
                else: # if it's asking for versions that doesn't exist, return base page
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1:
                            data = self.table.bufferpool.get_page(self.table.name, base_page_index+i+4, True).read_val(key)
                            columns.append(data)
            new_record = Record(key, search_key, columns)
            record_list.append(new_record)
            base_page_index = (key // max_records)*(self.table.num_columns+4)     
        return record_list
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns):
        rid_list = self.table.index.locate(self.table.key, key)
        if rid_list != []:
            key_rid = (self.table.index.locate(self.table.key, key))[0] #get the row number of the inputted key

            max_records = self.table.max_records #this is defined in the page class as 64 records
            page_set = key_rid // max_records #select the base page (row of physical pages) that row falls in
            
            tail_rid = 0
            num_tail_pages = 0
            with self.table.update_thread_lock:
                tail_rid = self.table.total_tail_records
                self.table.total_tail_records += 1
                if (tail_rid != 0 and tail_rid % self.table.max_records == 0): #if there's no capacity
                    self.table.init_tail_page_dir() #add one tail page (a set of physical pages, one for each column)
                num_tail_pages = self.table.num_tail_pages
            pages_start = (num_tail_pages+1) - (self.table.num_columns+5)

            # write the first 4 columns of the tail record: indirection column, rid, schema_encoding, and time_stamp
            # make the indirection column of the tail record hold the rid currently held in the base record's indirection column
                # tail record of indirection column will then point to the prev version of data -> will be -1 if the prev version is the base record, based on our implementation of insert_record
            prev_version_rid = self.table.bufferpool.get_page(self.table.name, page_set*(self.table.num_columns+4), True).read_val(key_rid)
            self.table.bufferpool.get_page(self.table.name, pages_start, False).write(prev_version_rid, tail_rid)
            #print("indirection column should be ",)
            self.table.bufferpool.get_page(self.table.name, 1+pages_start, False).write(tail_rid, tail_rid) #Writing to the tail's rid column
            self.table.bufferpool.get_page(self.table.name, 2+pages_start, False).write(0, tail_rid)
            self.table.bufferpool.get_page(self.table.name, 3+pages_start, False).write(0, tail_rid)
            
            # write the actual data columns of the tail record
            data = []
            if (prev_version_rid == -1): # reference the base record during the update
                for i in range(self.table.num_columns):
                    value = self.table.bufferpool.get_page(self.table.name, i+4+page_set*(self.table.num_columns+4), True).read_val(key_rid)
                    if (columns[i] != None):
                        self.table.index.delete_index(i, value, key_rid)
                        self.table.index.add_index(i, columns[i], key_rid)
                        value = columns[i]
                    self.table.bufferpool.get_page(self.table.name, i+4+pages_start, False).write(value, tail_rid)
                    data.append(self.table.bufferpool.get_page(self.table.name, i+4+pages_start, False).read_val(tail_rid))
            else: # reference the prev_tail_record during the update
                prev_tpage_set = prev_version_rid // max_records
                for i in range(self.table.num_columns):
                    value = self.table.bufferpool.get_page(self.table.name, i+4+prev_tpage_set*(self.table.num_columns+5), False).read_val(prev_version_rid)
                    if (columns[i] != None):
                        self.table.index.delete_index(i, value, key_rid)
                        self.table.index.add_index(i, columns[i], key_rid)
                        value = columns[i]
                    self.table.bufferpool.get_page(self.table.name, i+4+pages_start, False).write(value, tail_rid)
                    data.append(self.table.bufferpool.get_page(self.table.name, i+4+pages_start, False).read_val(tail_rid))
            self.table.bufferpool.get_page(self.table.name, self.table.num_columns+4+pages_start, False).write(key_rid, tail_rid)
            #update indirection column of base record
            self.table.bufferpool.get_page(self.table.name, page_set*(self.table.num_columns+4), True).overwrite(key_rid, tail_rid)
            columns = []
            #update schema encoding column of base record
            self.table.bufferpool.get_page(self.table.name, 3+page_set*(self.table.num_columns+4), True).overwrite(key_rid, 1)
            return True
        else:
            return False  # if primary key not found

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start, end, column_index):
        total_sum = 0
        max_records = self.table.max_records #64 records
        # get all rid's within list
        rid_list = self.table.index.locate_range(column_index, start, end)
        if len(rid_list) == 0:
            return None

        for rid in rid_list:
            base_page_index = (rid // max_records)*(self.table.num_columns+4)
            indirection = self.table.bufferpool.get_page(self.table.name, base_page_index, True).read_val(rid) # other version: change to only base_page_index
            if indirection == -1 or indirection < self.table.tps: # has not been updated (return record in base page)
                data = self.table.bufferpool.get_page(self.table.name, base_page_index+column_index+4, True).read_val(rid)
                total_sum += data
            else: # has been updated, get tail page (return record in tail page)
                tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                data = self.table.bufferpool.get_page(self.table.name, tail_page_index+column_index+4, False).read_val(indirection)
                total_sum += data

        if total_sum:
            return total_sum
        else:
            return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start, end, column_index, version_num):
        total_sum = 0
        max_records = self.table.max_records #64 records
        # get all rid's within list
        rid_list = self.table.index.locate_range(column_index, start, end)
        if len(rid_list) == 0:
            return None
        for rid in rid_list:
            base_page_index = (rid // max_records)*(self.table.num_columns+4)
            indirection = self.table.bufferpool.get_page(self.table.name, base_page_index, True).read_val(rid)
            if indirection == -1 or indirection < self.table.tps: # has not been updated (return record in base page)
                data = self.table.bufferpool.get_page(self.table.name, base_page_index+column_index+4, True).read_val(rid)
                total_sum += data
            else: # has been updated, get tail page (return record in tail page)
                counter = -version_num # how many times we have to go back
                has_past = True # if there is more versions before the current tail record
                while(counter > 0 and has_past): # keep going back until it reaches the desired version
                    tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                    indirection = self.table.bufferpool.get_page(self.table.name, tail_page_index, False).read_val(indirection)
                    counter -= 1
                    if indirection == -1 or indirection < self.table.tps:
                        has_past = False
                if has_past:
                    tail_page_index = (indirection // max_records)*(self.table.num_columns+5)
                    data = self.table.bufferpool.get_page(self.table.name, tail_page_index+column_index+4, False).read_val(indirection)
                    total_sum += data
                else: # if it's asking for versions that doesn't exist
                    data = self.table.bufferpool.get_page(self.table.name, base_page_index+column_index+4, True).read_val(rid)
                    total_sum += data
        if total_sum:
            return total_sum
        else:
            return False
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False