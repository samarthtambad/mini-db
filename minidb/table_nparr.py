import operator
import numpy as np
from minidb.index import Index

OPERATORS = {
    "<": operator.lt, ">": operator.gt, "=": operator.eq, "!=": operator.ne,
    "≥":   operator.ge, "≤": operator.le, "and": operator.and_, "or": operator.or_
}

NUMERIC = {
    operator.lt: True, operator.gt: True, operator.eq: False, operator.ne: False,
    operator.ge: True, operator.le: True, operator.and_: False, operator.or_: False
}


# noinspection PyPep8Naming
class Table:

    def __init__(self, name, columns):
        self.name = name
        self.num_columns = len(columns)
        self.num_rows = 0
        self.index = {}
        self.header = np.array([columns])
        self.rows = np.empty([0, self.num_columns])
        self.col_names = {}
        self.col_dtypes = {}
        for idx, col in enumerate(columns):
            self.col_names[col] = idx

    def __is_col_int(self, idx):
        try:
            int(self.rows[0][idx])
            return True
        except ValueError:
            return False

    def __is_col_float(self, idx):
        try:
            float(self.rows[0][idx])
            return True
        except ValueError:
            return False

    def __get_col_with_dtype(self, idx):
        if self.__is_col_int(idx):
            return self.rows[:, idx].astype(int)
        elif self.__is_col_float(idx):
            return self.rows[:, idx].astype(float)
        else:  # return as string
            return self.rows[:, idx]

    def __get_length(self):
        return len(self.rows)

    def __auto_increment(self):
        self.num_rows += 1
        return self.num_rows

    def __get_column_idx(self, col_name):
        if col_name in self.col_names:
            return self.col_names[col_name]
        else:
            return None

    def __get_max_col_width(self):
        col_width=1;
        for col_idx,dtype in self.col_dtypes.items():
            if (dtype):
                w = len(max(self.rows[:,col_idx], key=len))
                if col_width<w:
                    col_width=w
        return col_width+2

    def get_dimensions(self):
        return self.rows.shape

    def set_dtypes(self):
        for col in list(self.col_names.keys()):
            idx=self.__get_column_idx(col)
            if (self.__is_col_int(idx)):
                self.col_dtypes[idx]=1
            elif (self.__is_col_float(idx)):
                # set to 0 so condition can be checked as a bool
                self.col_dtypes[idx]=0
            else:
                self.col_dtypes[idx]=2

    def insert_row(self, new_row):
        self.rows = np.concatenate((self.rows, new_row))
        self.__auto_increment()

    def print(self, f=None, num_rows=None):
        """ print contents of the table
        :param f: file to print to. Prints to stdout if None
        :param num_rows: num of rows to print
        :return: None
        """
        self.print_columns(f)
        if num_rows is None:
            num_rows = self.num_rows
        for i in range(0, num_rows):
            for idx, value in enumerate(self.rows[i]):
                if idx != 0:
                    print(" | ", end='', file=f)
                print(value, end='', file=f)
            print("", file=f)

    def print_columns(self, f=None):
        """ print column names (separated by |)
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        for idx, name in enumerate(self.col_names):
            if idx != 0:
                print(" | ", end='', file=f)
            print(name, end='', file=f)
        print("", file=f)

    def print_formatted(self, f=None, *args,**kwargs):
        """ print contents of the table
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        col_width=self.__get_max_col_width()
        # print header
        self.print_columns_formatted(col_width,f)
        if "num_rows" in kwargs:
            num_rows = kwargs["num_rows"]
        else:
            num_rows = self.num_rows
        # print table rows (separated by |)
        for i in range(0, num_rows):
            for idx, value in enumerate(self.rows[i]):
                if idx != 0:
                    print("", end='', file=f)
                print(str(value).ljust(col_width), end='', file=f)
            print("")

    def print_columns_formatted(self,col_width, f=None):
        """ print column names (separated by |)
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        for idx, name in enumerate(self.col_names):
            if idx != 0:
                print("", end='', file=f)
            print(name.ljust(col_width), end='', file=f)
        print("")

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

    def sort(self, result_table_name, columns):
        result_table = Table(result_table_name, self.col_names)
        idx = []
        for col in columns:
            if col not in self.col_names:
                print("Invalid command. Column not present in table")
                return None
            else:
                i = self.__get_column_idx(col)
                idx.insert(0, self.__get_col_with_dtype(i))
        
        order = np.lexsort(idx)
        sorted_rows = self.rows[order]
        result_table.rows = sorted_rows
        result_table.num_rows = len(sorted_rows)
        return result_table

    def select_join(self, criteria):
        print("select_join")
        for i in range(0, criteria.num_conditions):
            idx1 = self.__get_column_idx(criteria.conditions[i][0])
            idx2 = self.__get_column_idx(criteria.conditions[i][1])
            if idx1 is None:
                print("column %s is not present in table %s" % (criteria.conditions[i][0], self.name))
                return False
            if idx2 is None:
                print("column %s is not present in table %s" % (criteria.conditions[i][1], self.name))
                return False

            comparator = OPERATORS[criteria.comparators[i]]
            c_new = comparator(self.rows[:, idx1], self.rows[:, idx2])

            if i - 1 < 0:
                c = c_new
            else:
                logic_operator = OPERATORS[criteria.logic_operators[i-1]]
                c = logic_operator(c_new, c)

            return self.rows[np.where(c)]

    def select(self, criteria):
        # perform select. select subset of rows and return resulting table
        for i in range(0, criteria.num_conditions):
            idx = self.__get_column_idx(criteria.conditions[i][0])
            if idx is None:
                print("column %s is not present in table %s" % (criteria.conditions[i][0], self.name))
                return False
            
            comparator = OPERATORS[criteria.comparators[i]]
            val = criteria.conditions[i][1]

            if NUMERIC[comparator]:
                c_new = comparator(self.rows[:, idx].astype(int), int(val))
            else:
                c_new = comparator(self.rows[:, idx], val)

            if i - 1 < 0:
                c = c_new
            else:
                logic_operator = OPERATORS[criteria.logic_operators[i-1]]
                c = logic_operator(c_new, c)
        
        return self.rows[np.where(c)]

    def avg(self,out_table_name, column):
        # will average have multiple columns?
        result_table = Table(out_table_name, ["avg_"+column])
        idx = self.__get_column_idx(column)
        avg = np.mean(self.rows[:, idx].astype(float))
        avg="{:.4f}".format(avg)
        result_table.insert_row([[avg]])
        return result_table

    def sum(self,out_table_name, column):
        # will average have multiple columns?
        result_table = Table(out_table_name, ["sum_"+column])
        idx = self.__get_column_idx(column)
        s = np.sum(self.rows[:, idx].astype(float))
        s="{:.4f}".format(s)
        result_table.insert_row([[s]])
        return result_table

    def count(self, out_table_name):
        result_table=Table(out_table_name, ["count"])
        result_table.insert_row([[self.__get_length()]])
        return result_table

    def group(self, columns):
        projection = self.projection("projection", columns)
        keys, indices = np.unique(projection.rows, axis=0, return_inverse=True)
        # print(keys)
        groups = [[] for i in range(len(keys))]
        for i, k in enumerate(indices):
            groups[k].append(self.rows[i])
        groups = [np.array(x) for x in groups]
        return keys, groups

    def avggroup(self, out_table_name, avg_column, groupby_columns):
        result_table = Table(out_table_name, ["avg_"+avg_column] + groupby_columns)
        avg_idx = self.__get_column_idx(avg_column)
        keys, groups = self.group(groupby_columns)
        for i in range(0,len(groups)):
            s = np.mean(groups[i][:, avg_idx].astype(float))
            new_row = np.insert(keys[i], 0, "{:.4f}".format(s))
            result_table.insert_row([new_row])
        return result_table

    def sumgroup(self, out_table_name, sum_column, groupby_columns):
        result_table = Table(out_table_name, ["sum_" + sum_column] + groupby_columns)
        sum_idx = self.__get_column_idx(sum_column)
        keys, groups = self.group(groupby_columns)
        for i in range(0, len(groups)):
            if self.__is_col_int(sum_idx):
                s = np.sum(groups[i][:, sum_idx].astype(int))
            else:
                s = np.sum(groups[i][:, sum_idx].astype(float))
            new_row = np.insert(keys[i], 0, s)
            result_table.insert_row([new_row])
        return result_table

    def movavg(self, out_table_name, column, n):
        result_table = Table(out_table_name, list(self.col_names.keys()) + ["mov_avg"])
        weights = np.ones(n)
        c = self.rows[:, self.__get_column_idx(column)].astype(float)
        c = np.concatenate((np.zeros(n - 1), c), axis=None)
        o = np.concatenate((np.zeros(n - 1), np.ones(len(c))))
        sum_vec = np.convolve(c, weights, 'valid')
        div_vec = np.convolve(o, weights, 'valid')
        avg_vec = [x/y for x, y in zip(sum_vec, div_vec)]

        avg_vec = np.vstack(avg_vec)
        result_table.rows = np.array(avg_vec)
        result_table.num_rows = len(avg_vec)
        return result_table

    def movsum(self, out_table_name, column, n):
        result_table = Table(out_table_name, list(self.col_names.keys()) + ["mov_sum"])
        weights = np.ones(n)
        c = self.rows[:, self.__get_column_idx(column)].astype(float)
        c = np.concatenate((np.zeros(n - 1), c), axis=None)
        sum_vec = np.convolve(c, weights, 'valid')

        sum_vec = np.vstack(sum_vec)
        result_table.rows = np.array(sum_vec)
        result_table.num_rows = len(sum_vec)
        return result_table

    def btree_index(self, column):
        index = Index(self, self.__get_column_idx(column), "Btree")
        index.print()
        self.index[column] = index

    def hash_index(self, column):
        index = Index(self, self.__get_column_idx(column), "Hash")
        index.print()
        self.index[column] = index

    def index_list(self):
        for key, idx in self.index.items():
            print("%-15s %-15s %-15s" % (self.name, key, idx.type))
