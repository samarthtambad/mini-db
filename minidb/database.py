
from minidb.table_nparr import Table
import copy


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

    # print out tables currently present in the database
    def show_tables(self):
        if len(self.tables) == 0:
            print("No tables")
        else:
            for table in self.tables:
                print(table)

    def input_from_file(self, table_name, file):
        """ Import data from given vertical bar delimited `file`
        into array-table. (1 or more columns)
        corresponds to:
            CREATE TABLE `table_name` (...)
            LOAD DATA INFILE `file` INTO TABLE `table_name` FIELDS TERMINATED BY '|'
        :param table_name: name of the table to create
        :param file: path to the input file.
        :return: success True/False
        """
        # print("input_from_file()")
        # TODO: Handle possible exceptions, return False
        # TODO: What to do if table already exists?
        table = None
        first = True
        try:
            with open(file, "r") as f:
                for line in f:
                    split = line.split("|")
                    split = [s.strip() for s in split]
                    if first:
                        first = False
                        table = Table(table_name, split)
                        continue
                    else:
                        try:
                            # key, values = split[0], split[1:]
                            # print(split)
                            table.insert_row(split)
                        except:
                            continue
            table.print()
            self.tables[table_name] = table
            self.table = table
            return True
        except OSError as e:
            print(e)
            return False

    def output_to_file(self, table_name, file):
        """ Output contents of `table` (with vertical bar separators) into `file`.
        :param table_name: name of the table to output
        :param file: path to the output file where output must be written.
        :return: success True/False
        """
        # print("output_to_file()")
        if table_name not in self.tables:
            print("No table found")
            return False
        table = self.tables[table_name]
        with open(file, "a") as f:
            table.print(f)
        return True

    def select(self, output, in_table, criteria):
        """ Select all columns from `table` satisfying the given `criteria`.
        Prints the result to standard output.
        :param table: name of the table to output
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        print("select()")
        # check to make sure table to be selected from exists
        if (in_table in self.tables):
            table=Table(output, self.tables[in_table].col_names)
            self.tables[in_table].select(criteria)

            # create new table with appropriate name  
            self.tables[output]=table
            return True
        
        else:
            print("Table %s does not exist" % in_table)
            return False

        # for row in self.tables[in_table]:

        
    
    # I have modified this function to create another table. That makes more
    # sense as we could also do operations on it.
    def project(self, projected_table_name, table_name, columns):
        """ select a subset of columns from a table
        :param projected_table_name: name of the projected table
        :param table_name: name of the table from which to select columns
        :param columns: columns to keep in the projection
        :return: success True/False
        """
        print("project()")
        if table_name not in self.tables:
            print("No table found")
            return False
        columns = [s.strip() for s in columns]
        projection: Table = self.tables[table_name].projection(projected_table_name, columns)
        if projection is None:
            return False
        self.tables[projected_table_name] = projection
        projection.print()
        return True
    
    def concat(self, output, tables):  # TODO: ensure schemas are the same
        """ concatenate tables (with the same schema)
        :param tables: list of tables to be concatenated
        :return: None
        """
        print("concat()")

        # create a copy of the first table
        table=copy.deepcopy(self.tables[tables[0]])
        for row in self.tables[tables[1]].rows[1:]:
            table.insert_row(row.data)
        # save concatenated table in database with appropriate name
        self.tables[output]=table
        table.print()

    def sort(self, output, table, columns):
        """ sort `table` by each column in `columns` in the given order
        :param table: name of the table
        :param columns: name of the columns to sort by (in the given order)
        :return: None
        """
        print("sort()")

        table = None
        self.tables[output] = table

    def join(self, output, tables, criteria):
        """ select all columns from each of the `tables'.
        Filter rows by ones that satisfy the `criteria`
        :param tables: names of the tables
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        print("join()")
        t1=self.tables[tables[0]]
        t2=self.tables[tables[1]]
        
        # create new table with appropriate name and columns
        t1_cols=[tables[0]+"_"+x for x in t1.col_names]
        t2_cols=[tables[1]+"_"+x for x in t2.col_names]
        table = Table(output,t1_cols+t2_cols)
        self.tables[output] = table

        # create projections for each table, create cross product of arrays

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
