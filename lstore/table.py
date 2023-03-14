from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None
        self.schema_encoding = None
        self.exists = True

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.base_pages = [0]
        self.tail_pages = [[1]] * num_columns
        self.pages = []
        self.next_rid = 0
        for i in range(num_columns):
            self.pages.append([Page(), Page()])
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
