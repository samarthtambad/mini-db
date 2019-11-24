from BTrees.OOBTree import OOBTree

# TODO: discuss whether this implementation is efficient enough. Else, how to optimize? Or is it better to go with np.array?


class Table:

    def __init__(self, columns):
        self.num_columns = len(columns)
        self.rows = OOBTree()  # primary index
        self.col_names = {}
        for idx, col in enumerate(columns):
            self.col_names[col] = idx

    def __get_column_idx(self, col_name):
        return self.col_names[col_name]

    def projection(self, *columns):
        idx = []

        # create a list of indexes of given columns
        for col in columns:
            if col not in self.col_names:
                print("Invalid command. Column not present in table")
                return False
            idx.append(self.__get_column_idx(col))

        # return a sequence of rows but only include columns with index in `idx`
        result = []
        for items in self.rows.values():
            row = []
            for i in idx:
                row.append(items[i])
            result.append(row)

        return result

    def insert_row(self, key, values):
        """insert a row into this table
        corresponds to: INSERT INTO TABLE VALUES(key, value1, value2, ...)
        :param key: primary key of the table
        :param values: all the other columns in the table
        :return: success, true/false
        """
        if 1 + len(values) != self.num_columns:
            print("Invalid command. Number of columns don't match")
            return False

        if key in self.rows:
            print("Invalid command. Primary key cannot have duplicated")
            return False

        columns = [key]     # TODO: decide if needed or not. I kept it for ease of handling index
        for value in values:
            columns.append(value)
        self.rows.update({key: columns})
        return True

    def print(self):
        # print column names (separated by |)
        for idx, name in enumerate(self.col_names):
            if idx != 0:
                print(" | ", end='')
            print(name, end='')
        # print table rows (separated by |)
        for values in self.rows.values():
            for idx, value in enumerate(values):
                if idx != 0:
                    print(" | ", end='')
                print(value, end='')
        pass
