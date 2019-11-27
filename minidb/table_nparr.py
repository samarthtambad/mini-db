import numpy as np
import os

"""changelog:
1. self.columns not being used. removed.
2. changed self.size to self.num_rows. more readable.
3. made __auto_increment as a way to update self.num_rows
4. made get_length private. no need for this to be accessible outside 
4. 

To-do
add sort as a method in table
add Hash and Btree as a method in table
add indexes as attribute in table

Questions
1. Do we need to add a surrogate key?

"""

# print(self.rows.shape)
# print(self.header.shape)
# self.rows=np.concatenate((self.rows,np.array(self.header)),axis=0)


class Table:

    def __init__(self, name, columns):
        self.name = name
        self.num_columns = len(columns)
        self.num_rows = 0
        self.header = np.array([columns])
        self.rows = np.empty([0, self.num_columns])
        self.col_names = {}
        for idx, col in enumerate(columns):
            self.col_names[col] = idx

    def __get_length(self):
        return len(self.rows)

    def __auto_increment(self):
        self.num_rows += 1
        return self.num_rows

    def __get_column_idx(self, col_name):
        return self.col_names[col_name]

    def sort(self):
        pass

    def select(self, criteria):
        print("inner select()")

    def projection(self, name, columns):
        # if given list of column names is beyond this table
        if len(columns) > self.num_columns:
            return None

        idx = []
        projected_table = Table(name, columns)

        # create a list of indexes of given columns
        for col in columns:
            if col not in self.col_names:
                print("Invalid command. Column not present in table")
                return None
            idx.append(self.__get_column_idx(col))

        # insert a row, but only include columns with index in `idx`
        for row in self.rows:
            new_row = []
            for i in idx:
                new_row.append(row[i])
            projected_table.insert_row(np.array([new_row]))

        return projected_table
            
    def insert_row(self, new_row):
        self.rows = np.concatenate((self.rows, new_row))
        self.__auto_increment()

    def print(self, f=None):
        """ print contents of the table
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        self.print_columns(f)
        # print table rows (separated by |)
        for i in range(0, self.num_rows):
            for idx, value in enumerate(self.rows[i]):
                if idx != 0:
                    print(" | ", end='', file=f)
                print(value, end='', file=f)
            print("")

    def print_columns(self, f=None):
        """ print column names (separated by |)
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        for idx, name in enumerate(self.col_names):
            if idx != 0:
                print(" | ", end='', file=f)
            print(name, end='', file=f)
        print("")

