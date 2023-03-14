from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        if primary_key in self.table.index.indices[self.table.key].keys():
            if self.table.page_directory[self.table.index.indices[self.table.key][primary_key]].exists:
                self.table.page_directory[self.table.index.indices[self.table.key][primary_key]].exists = False
                return True
        return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if columns[self.table.key] in self.table.index.indices[self.table.key].keys():
            return False
        RID = self.table.next_rid
        self.table.next_rid += 1
        data_cols = []
        if not self.table.pages[0][self.table.base_pages[-1]].has_capacity():
            self.table.base_pages.append(len(self.table.pages[0]))
            for i in self.table.pages:
                i.append(Page())
        for i in range(len(columns)):
            data_cols.append((i, self.table.pages[i][self.table.base_pages[-1]]))
            self.table.pages[i][self.table.base_pages[-1]].write(columns[i])
        base_record = Record(RID, columns[self.table.key], data_cols)
        base_record.schema_encoding = '0' * self.table.num_columns
        self.table.index.indices[self.table.key][columns[self.table.key]] = RID
        self.table.page_directory[RID] = base_record
        return True

    
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
        RIDs = self.table.index.locate(search_key_index, search_key)
        data = []
        for RID in RIDs:
            row = self.table.page_directory[RID]
            columns = []
            for i in range(self.table.num_columns):
                if projected_columns_index[i] == 1:
                    if row.schema_encoding[i] == '1':
                        value = self.table.page_directory[row.indirection].columns[i]
                        columns.append(int(value[1].data[value[0]*8,(value[0]*8)+7]))
                    else:
                        value = row.columns[i]
                        columns.append(int(value[1].data[value[0]*8,(value[0]*8)+7]))
            if row.schema_encoding[self.table.key] == '1':
                data.append(Record(RID, self.table.page_directory[row.indirection].key, columns)
            else:
                data.append(Record(RID, row.key, columns)
        return data

    
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
        RIDs = self.table.index.locate(search_key_index, search_key)
        data = []
        for RID in RIDs:
            base_row = self.table.page_directory[RID]
            row = self.table.page_directory[base_row.indirection]
            for i in range(relative_version):
                if row.indirection == None:
                    row = base_row
                    break
                row = self.table.page_directory[row.indirection]
            columns = []
            for i in range(self.table.num_columns):
                if projected_columns_index[i] == 1:
                    value = row.columns[i]
                    columns.append(int(value[1].data[value[0]*8,(value[0]*8)+7]))
            if row.schema_encoding[self.table.key] == '1':
                data.append(Record(RID, self.table.page_directory[row.indirection].key, columns)
            else:
                data.append(Record(RID, row.key, columns)
        return data

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        if columns[self.table.key] in self.table.index.indices[self.table.key].keys() and (columns[self.table.key] != primary_key):
            return False
        RID = self.table.next_rid
        self.table.next_rid += 1
        data_cols = []
        base_record = self.table.page_directory[self.table.index.indices[self.table.key][primary_key]]
        schema_encoding = ''
        for i in range(len(columns)):
            if columns[i] == None:
                schema_encoding += base_record.schema_encoding[i]
                if base_record.schema_encoding[i] == '1':
                    data_cols.append(self.table.page_directory[base_record.indirection].columns[i])
                else:
                    data_cols.append(base_record.columns[i])
            else:
                schema_encoding += '1'
                if not self.table.pages[i][self.table.tail_pages[i][0]].has_capacity():
                    self.table.tail_pages[i].pop(0)
                    if len(self.table.tail_pages[i]) == 0:
                        for i in self.table.tail_pages:
                            i.append(len(self.table.pages[0]))
                        for i in self.table.pages:
                            i.append(Page())
                data_cols.append((i, self.table.pages[i][self.table.tail_pages[i][0]]))
                self.table.pages[i][self.table.tail_pages[i][0]].write(columns[i])
        tail_record = Record(RID, data_cols[self.table.key], data_cols)
        tail_record.schema_encoding = schema_encoding
        self.table.page_directory[self.table.index.indices[self.table.key][primary_key]].schema_encoding = schema_encoding
        self.table.page_directory[self.table.index.indices[self.table.key][primary_key]].indirection = RID
        if columns[self.table.key] != None:
            self.table.index.indices[self.table.key][columns[self.table.key]] = base_record.rid
        self.table.page_directory[RID] = tail_record
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
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
