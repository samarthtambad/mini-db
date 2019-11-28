
from minidb.table_nparr import Table
import copy
import numpy as np
from minidb.argparser import ArgParser


"""
Changelog
1. added function __save_table. makes code more readable.
2. also added __get_table and __exists for same reason. We 
   won't be using self.tables directly anymore
3. self.table not being used. removed.
4. 

Notes
1. Let's not use Table() anywhere else except input_from_file. Because for each of the other 
   commands, the table needs to return its subset which is better handled in Table itself.
2. movesum and moveavg still have unhandled problems

Questions
1. If table with table_name is already present in the map, what to do? Overwrite or throw error?
2. For concat, where are we checking schema (two schemas must be the same)?

"""


class Database:

    def __init__(self):
        self.tables = {}

    def show_tables(self):
        """print out tables currently present in the database
        :return: None
        """
        if len(self.tables) == 0:
            print("No tables")
        else:
            for table in self.tables:
                print(table)

    def __exists(self, table_name):
        """ check if table exists in database
        :param table_name: name of the table
        :return: True / False
        """
        if table_name in self.tables:
            return True
        return False

    def __save_table(self, table_name, table):
        """save table with table_name into table map
        :param table_name: name of the table
        :param table: reference to Table object
        :return: None
        """
        if table_name in self.tables:
            print("Table already present. Overwriting...")
        self.tables[table_name] = table

    def __get_table(self, table_name):
        """ get Table object mapped with name table_name
        :param table_name: name of the table
        :return: Table / None
        """
        if table_name not in self.tables:
            print("Table", table_name, "not present in database")
            return None
        table: Table = self.tables[table_name]
        return table

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
                            new_row = np.array([split])
                            # print(new_row)
                            table.insert_row(new_row)
                        except Exception as e:
                            print(e)
                            continue
            table.print()
            self.__save_table(table_name, table)
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
        if not self.__exists(table_name):
            print("No table found")
            return False
        table = self.__get_table(table_name)
        with open(file, "a") as f:
            table.print(f)
        return True

    def select(self, out_table_name, in_table_name, criteria):
        """ Select all columns from `table` satisfying the given `criteria`.
        Prints the result to standard output.
        :param table: name of the table to output
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        # print("select()")
        if not self.__exists(in_table_name):
            print("Table %s does not exist" % in_table_name)
            return False

        in_table=self.__get_table(in_table_name)

        out_table = Table(out_table_name,in_table.col_names.keys())
        data=in_table.select(criteria)
        out_table.rows=data
        out_table.num_rows=len(data)
        out_table.print()

        # create new table with appropriate name
        self.__save_table(out_table_name, out_table)
        return True

    def project(self, projected_table_name, in_table_name, columns):
        """ select a subset of columns from a table
        :param projected_table_name: name of the projected table
        :param in_table_name: name of the table from which to select columns
        :param columns: columns to keep in the projection
        :return: success True/False
        """
        # print("project()")
        if not self.__exists(in_table_name):
            print("No table found")
            return False

        columns = [s.strip() for s in columns]
        projection: Table = self.tables[in_table_name].projection(projected_table_name, columns)
        if projection is None:
            return False
        self.__save_table(projected_table_name, projection)
        projection.print()
        return True
    
    def concat(self, out_table_name, tables):  # TODO: ensure schemas are the same
        """ concatenate tables (with the same schema)
        :param tables: list of tables to be concatenated
        :return: None
        """
        # print("concat()")
        # create a copy of the first table
        table1 = self.__get_table(tables[0])
        table2 = self.__get_table(tables[1])
        table = copy.deepcopy(table1)
        for row in table2.rows[1:]:
            table.insert_row([row])
        # save concatenated table in database with appropriate name
        self.__save_table(out_table_name, table)
        table.print()

    def sort(self, out_table_name, in_table_name, columns):
        """ sort `table` by each column in `columns` in the given order
        :param out_table_name: name of the resulting table
        :param columns: name of the columns to sort by (in the given order)
        :return: None
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False

        in_table=self.__get_table(in_table_name)
        out_table = Table(out_table_name,in_table.col_names.keys())
        data=in_table.sort(columns)
        out_table.rows=data
        out_table.num_rows=len(data)
        out_table.print()
        self.__save_table(out_table_name, out_table)

    def join(self, out_table_name, tables, criteria):
        """ select all columns from each of the `tables'.
        Filter rows by ones that satisfy the `criteria`
        :param out_table_name: name of the resulting table
        :param tables: list of tables to join
        :param criteria: condition(s) that each selected row must satisfy
        :return: None
        """
        # print("join()")
        t1 = self.__get_table(tables[0])
        t2 = self.__get_table(tables[1])

        # create new table with appropriate name and columns
        t1_cols = [tables[0] + "_" + x for x in t1.col_names]
        t2_cols = [tables[1] + "_" + x for x in t2.col_names]
        table = Table(out_table_name, t1_cols + t2_cols)

        temp=Table("cartesian_product", t1_cols + t2_cols)
        for t1_row in t1.rows:
            for t2_row in t2.rows:
                new_row=np.append(t1_row,t2_row)
                temp.insert_row([new_row])
        # temp.print()
        criteria.join_to_select()
        data=temp.select_join(criteria)
        table.rows=data
        table.num_rows=len(data)
        out_table.num_rows=len(data)
        # rows need to be added
        self.__save_table(out_table_name, table)
        table.print()

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

    def movavg(self, out_table_name, in_table_name, column, n):
        """ perform `n` item moving average over `column` of `table'
        :param out_table_name: name of the resulting table
        :param in_table_name: name of the input table
        :param column: name of the column
        :param n: number of items over which to take moving average
        :return: None
        """
        # print("movavg()")
        if not self.__exists(in_table_name):
            print("No table found")
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.movavg(out_table_name, column, n)
        self.__save_table(out_table_name, out_table)
        return out_table

    def movsum(self, out_table_name, in_table_name, column, n):
        """ perform `n` item moving sum over `column` of `table'
        :param out_table_name: name of the resulting table
        :param in_table_name: name of the input table
        :param column: name of the column
        :param n: number of items over which to take moving sum
        :return: None
        """
        # print("movsum()")
        if not self.__exists(in_table_name):
            print("No table found")
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.movsum(out_table_name, column, n)
        self.__save_table(out_table_name, out_table)
        return out_table


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
