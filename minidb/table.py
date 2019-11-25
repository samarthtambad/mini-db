from BTrees.OOBTree import OOBTree

# TODO: discuss whether this implementation is efficient enough. Else, how to optimize? Or is it better to go with np.array?


class Table:

    def __init__(self, name, columns):
        self.name = name
        self._surrogate_key = "_idx"
        self.counter = 0
        self.num_columns = len(columns)     # doesn't include surrogate key
        self.rows = OOBTree()  # primary index
        self.col_names = {}     # doesn't include surrogate key
        for idx, col in enumerate(columns):
            self.col_names[col] = idx + 1

    def __get_column_idx(self, col_name):
        return self.col_names[col_name]

    def __auto_increment(self):
        self.counter += 1
        return self.counter

    def projection(self, columns):
        idx = []

        # create a list of indexes of given columns
        for col in columns:
            if col not in self.col_names:
                print("Invalid command. Column not present in table")
                return None
            idx.append(self.__get_column_idx(col))

        # return a sequence of rows but only include columns with index in `idx`
        result = []
        for items in self.rows.values():
            row = []
            for i in idx:
                row.append(items[i])
            result.append(row)

        return result

    def insert_row(self, values):
        """insert a row into this table
        corresponds to: INSERT INTO TABLE VALUES(key, value1, value2, ...)
        :param key: primary key of the table
        :param values: all the other columns in the table
        :return: success, true/false
        """
        if len(values) != self.num_columns:
            print("Invalid command. Number of columns don't match")
            return False

        key = self.__auto_increment()
        self.rows.update({key: values})
        return True

    def print(self, f=None):
        # print column names (separated by |)
        for idx, name in enumerate(self.col_names):
            if idx != 0:
                print(" | ", end='', file=f)
            print(name, end='', file=f)
        # print table rows (separated by |)
        for values in self.rows.values():
            for idx, value in enumerate(values):
                if idx != 0:
                    print(" | ", end='', file=f)
                print(value, end='', file=f)
        pass
