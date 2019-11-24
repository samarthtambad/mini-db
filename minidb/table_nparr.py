import numpy as np
import os


class Table:
    def __init__(self, columns):
        self.table = np.array(object)
        self.header = None

    def get_length(self):
        return len(self.table)

    def __get_column_idx(self, col_name):
        # TODO: implement
        return

    def projection(self):
        # TODO: implement
        return

    def create_table_from_file(self, filename):
        with open(filename) as fp:
            row = fp.readline() #this will be the header
            while row:
                new_row = Row()
                for field in row.split("|"):
                    new_row.data.append(field)
                self.insert_row(new_row)
                row = fp.readline()
            
    def insert_row(self, row):
        self.table = np.append(self.table, row)
        
    def print(self):
        for i in range(0, len(self.table)):
            print(self.table[i])


class Row:
    def __init__(self):
        self.data = []
    
    def __str__(self):
        row_str = ""
        for field in self.data:
            row_str += field + "|"
        return row_str[:-1]
