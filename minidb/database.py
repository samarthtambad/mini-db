
from table import Table


class Database:

    def __init__(self):
        self.tables = {}
        self.table = None  # keeping temporarily for testing

    """TODO, Remove before submitting
    You will hand in clean and well structured source code in which each 
    function has a header that says: 
    (i) what the function deos, 
    (ii) what its inputs are and what they mean 
    (iii) what the outputs are and mean 
    (iv) any side effects to globals.
    """

    def input_from_file(self, table_name, file):
        """ Import data from given vertical bar delimited `file`
        into array-table. (1 or more columns)
        :param file: path to the input file.
        :return: reference to the created table
        """
        # print("input_from_file()")
        table = None
        first = True
        with open(file, "r") as f:
            for line in f:
                split = line.split("|")
                if first:
                    first = False
                    table = Table(split)
                    continue
                else:
                    try:
                        key, values = split[0], split[1:]
                        table.insert_row(key, values)
                    except:
                        continue
        table.print()
        self.tables[table_name] = table
        self.table = table
        return table

    def output_to_file(self, table, file):
        """ Output contents of `table` (with vertical bar separators) into `file`.
        :param table: name of the table to output
        :param file: path to the output file where output must be written.
        :return: None
        """
        print("output_to_file()")

    def select(self, table, criteria):
        """ Select all columns from `table` satisfying the given `criteria`.
        Prints the result to standard output.
        :param table: name of the table to output
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        print("select()")
    
    def project(self, table, *columns):  # TODO: I think this must also be a method of table
        """ select a subset of columns from a table
        :param table: name of the table from which to select columns
        :param columns: columns to keep in the projection
        :return: None
        """
        print("project()")
        if table not in self.tables:
            print("No table found")
            return None
        print(self.tables[table].projection("saleid", "itemid", "customerid", "storeid"))
    
    def concat(self, table1, table2):
        """ concatenate two tables (with the same schema)
        :param table1: name of the first table
        :param table2: name of the second table
        :return: None
        """
        print("concat()")
    
    def sort(self, table, columns):
        """ sort `table` by each column in `columns` in the given order
        :param table: name of the table
        :param columns: name of the columns to sort by (in the given order)
        :return: None
        """
        print("sort()")
    
    def join(self, tables, criteria):
        """ select all columns from each of the `tables'.
        Filter rows by ones that satisfy the `criteria`
        :param tables: names of the tables
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        print("join()")

    def avggroup(self, table, avg_column, other_columns):
        """ select avg(`sum_column`), `other_columns` from table
        :param table: name of the table
        :param avg_column: name of column over which avg is taken
        :param other_columns: names of other columns
        :return: None
        """
        print("avggroup()")

    def sumgroup(self, table, sum_column, other_columns):
        """ select sum(`sum_column`), `other_columns` from table
        :param table: name of the table
        :param sum_column: name of column over which sum is taken
        :param other_columns: names of other columns
        :return: None
        """
        print("sumgroup()")

    def movavg(self, table, column, n):
        """ perform `n` item moving average over `column` of `table'
        :param table: name of the table
        :param column: name of the column
        :param n: number of items over which to take moving average
        :return: None
        """
        print("movavg()")

    def movsum(self, table, column, n):
        """ perform `n` item moving sum over `column` of `table'
        :param table: name of the table
        :param column: name of the column
        :param n: number of items over which to take moving sum
        :return: None
        """
        print("movsum()")
    
    def avg(self, table, column):
        """ select avg(`column`) from `table`
        :param table: name of the table
        :param column: name of the column
        :return: None
        """
        print("avg()")

    def btree(self, table, column):
        """ create a Btree index on `table` based on `column`
        Note: all indexes will be based on 1 column
        :param table: name of the table
        :param column: name of the column
        :return: None
        """
        print("btree()")

    def hash(self, table, column):
        """ create a Hash index on `table` based on `column`
        Note: all indexes will be based on 1 column
        :param table: name of the table
        :param column: name of the column
        :return: None
        """
        print("hash()")
