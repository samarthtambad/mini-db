from BTrees.OOBTree import OOBTree


class Table:

    def __init__(self, columns):
        self.columns = columns
        self.num_columns = len(columns)
        self.data = OOBTree()   # primary index

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
        columns = []
        for value in values:
            columns.append(value)
        self.data.update({key: columns})
        return True

    def print(self):
        for key, values in self.data.iteritems():
            print(key, end='')
            for value in values:
                print(" |", value, end='')
        pass
