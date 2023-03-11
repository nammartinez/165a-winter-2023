"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.indices[table.key] = {}
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is not None and self.table.page_directory[self.indices[column][value][0]].exists:
            return self.indices[column][value]
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        RIDs = []
        if self.indices[column] is not None:
            for i in range(begin, end):
                if self.table.page_directory[self.indices[column][i][0]].exists:
                    RIDs = RIDs + self.indices[column][i]
            return RIDs
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
