from BTrees.OOBTree import OOBTree


class Table:

    def __init__(self, name, columns):
        self.name = name
        self._surrogate_key = "_idx"
        self.counter = 0
        self.num_columns = len(columns)     # doesn't include surrogate key
        self.rows = OOBTree()  # primary index
        self.col_names = {}     # doesn't include surrogate key
        for idx, col in enumerate(columns):
            self.col_names[col] = idx

    def __get_column_idx(self, col_name):
        return self.col_names[col_name]

    def select(self,criteria):
        print("__select")
        for i in  range(criteria.num_conditions):
            print(criteria.conditions[i])
            # print(criteria.comparators[i])
            col_idx=self.__get_column_idx(criteria.conditions[i][0])
            # print(self.__get_column_idx(criteria.conditions[i][0]))
            val=criteria.conditions[i][1]
            for row_idx,row in self.rows.items():
                # print(row)
                # print(row.value())
                if int(row[col_idx])>int(val):
                    print(row)

    def __auto_increment(self):
        self.counter += 1
        return self.counter

    # TODO: this should return another table
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
        for items in self.rows.values():
            row = []
            for i in idx:
                row.append(items[i])
            projected_table.insert_row(row)

        return projected_table

    # def projection(self, columns):
    #     idx = []
    #
    #     # create a list of indexes of given columns
    #     for col in columns:
    #         if col not in self.col_names:
    #             print("Invalid command. Column not present in table")
    #             return None
    #         idx.append(self.__get_column_idx(col))
    #
    #     # return a sequence of rows but only include columns with index in `idx`
    #     result = []
    #     for items in self.rows.values():
    #         row = []
    #         for i in idx:
    #             row.append(items[i])
    #         result.append(row)
    #
    #     return result

    def insert_row(self, values):
        """insert a row into this table
        corresponds to: INSERT INTO TABLE VALUES(value1, value2, ...)
        :param values: all the columns in the table
        :return: success, true/false
        """
        if len(values) != self.num_columns:
            print("Invalid command. Number of columns don't match")
            return False

        key = self.__auto_increment()
        self.rows.update({key: values})
        return True

    def print(self, f=None):
        """ print contents of the table
        :param f: file to print to. Prints to stdout if None
        :return: None
        """
        self.print_columns(f)
        # print table rows (separated by |)
        for values in self.rows.values():
            for idx, value in enumerate(values):
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
