import numpy as np
import os


class Table:
    def __init__(self, name, columns):
        self.name=name
        self.columns = columns
        self.num_columns=len(columns)
        self.col_names = {}
        self.rows = np.empty([0,self.num_columns])
        # print(self.rows.shape)
        self.header=np.array([columns])
        # print(self.header.shape)
        # self.rows=np.concatenate((self.rows,np.array(self.header)),axis=0)
        for idx, col in enumerate(columns):
            self.col_names[col] = idx
        self.size=0


    def get_length(self):
        return len(self.rows)

    def __auto_increment(self):
        self.counter += 1
        return self.counter

    def __get_column_idx(self, col_name):
        return self.col_names[col_name]
        return

    def select(self,criteria):
        print("inner select()")


    def projection(self,name, columns):
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
        return
            
    def insert_row(self, new_row):
        # new_row=np.empty()
        # for field in data.split("|"):
        #     new_row=np.append(append(field))

        self.rows = np.concatenate((self.rows, new_row))
        self.size+=1
        
    def print(self):
        # print(self.columns)
        for i in range(0, self.size):
            print(self.rows[i])




# class Row:
#     def __init__(self,data):
#         self.data = data
    
#     def __str__(self):
#         row_str = ""
#         for field in self.data:
#             row_str += field + "|"
#         return row_str[:-1]




#             def create_table_from_file(self, filename):
#         with open(filename) as fp:
#             row = fp.readline()  # this will be the header
#             while row:
#                 # new_row = Row()
#                 new_row=np.empty()
#                 for field in row.split("|"):
#                     # new_row.data.append(field)
#                     new_row=np.append(append(field))
#                 print(new_row)
#                 self.insert_row(new_row)
#                 row = fp.readline()
