import copy
import numpy as np
from minidb.table import Table
from minidb.join import Join


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

    def show_index(self):
        if len(self.tables) == 0:
            print("No tables")
        else:
            print("%-15s" % "INDEX LIST")
            print("%-15s %-15s %-15s" % ("TABLE", "COLUMN", "TYPE"))
            for table_name in self.tables:
                self.__get_table(table_name).index_list()

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
        # TODO: Handle possible exceptions, return False
        # TODO: What to do if table already exists?
        table = None
        first = True
        rows = []
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
                            rows.append(split)
                        except Exception as e:
                            print(e)
                            continue
            table.rows = np.array(rows)
            table.num_rows = len(rows)
            if (table.num_rows>0):
                table.set_dtypes()
            table.print(num_rows=5)
            self.__save_table(table_name, table)
            return True
        except OSError as e:
            print(e)
            return False

    def join(self, out_table_name, tables, criteria):
        """ select all columns from each of the `tables'.
        Filter rows by ones that satisfy the `criteria`
        :param out_table_name: name of the resulting table
        :param tables: list of tables to join
        :param criteria: condition(s) that each selected row must satisfy
        :return: success True/False
        """
        t1 = self.__get_table(tables[0])
        t2 = self.__get_table(tables[1])
        if t1 is None or t2 is None:
            return False

        # create new table with appropriate name and columns
        t1_cols = [tables[0] + "_" + x for x in t1.col_names]
        t2_cols = [tables[1] + "_" + x for x in t2.col_names]
        table = Table(out_table_name, t1_cols + t2_cols)

        join = Join(t1, t2, criteria)
        data=join.do_join()
        table.set_data(data)
        self.__save_table(out_table_name, table)
        # table.print()
        print("%d rows returned" % len(data))
        return True

    def output_to_file(self, table_name, file):
        """ Output contents of `table` (with vertical bar separators) into `file`.
        :param table_name: name of the table to output
        :param file: path to the output file where output must be written.
        :return: success True/False
        """
        table = self.__get_table(table_name)
        if table is None:
            return False
        with open(file, "a") as f:
            table.print(f)
        return True

    def select(self, out_table_name, in_table_name, criteria):
        """ Select all columns from `table` satisfying the given `criteria`.
        Prints the result to standard output.
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param criteria: condition(s) that each selected row must satisfy
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s does not exist" % in_table_name)
            return False

        in_table = self.__get_table(in_table_name)

        out_table = Table(out_table_name, in_table.col_names.keys())
        data = in_table.select(criteria)
        out_table.rows = data
        out_table.num_rows = len(data)
        out_table.print_formatted()
        print("%d rows returned" % len(data))
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
        if not self.__exists(in_table_name):
            print("No table found")
            return False

        columns = [s.strip() for s in columns]
        print(columns)
        projection: Table = self.tables[in_table_name].projection(projected_table_name, columns)
        if projection is None:
            return False
        self.__save_table(projected_table_name, projection)
        projection.print()
        return True
    
    def concat(self, out_table_name, tables):  # TODO: ensure schemas are the same
        """ concatenate tables (with the same schema)
        :param out_table_name: name of the output table
        :param tables: list of tables to be concatenated
        :return: None
        """
        # create a copy of the first table
        table1 = self.__get_table(tables[0])
        table2 = self.__get_table(tables[1])
        # table = copy.deepcopy(table1)
        table = table1.copy(out_table_name)
        for row in table2.rows[1:]:
            table.insert_row([row])
        # save concatenated table in database with appropriate name
        self.__save_table(out_table_name, table)
        table.print()

    def sort(self, out_table_name, in_table_name, columns):
        """ sort `table` by each column in `columns` in the given order
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param columns: name of the columns to sort by (in the given order)
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.sort(out_table_name, columns)
        out_table.print()
        print("%d rows returned" % len(out_table.rows))
        self.__save_table(out_table_name, out_table)
        return True

    def avggroup(self, out_table_name, in_table_name, avg_column, groupby_columns):
        """ select avg(`sum_column`), `other_columns` from table
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param avg_column: name of column over which avg is taken
        :param groupby_columns: names of columns to group by
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.avggroup(out_table_name, avg_column, groupby_columns)
        print("%d rows returned" % len(out_table.rows))
        self.__save_table(out_table_name, out_table)
        out_table.print()
        return True

    def sumgroup(self, out_table_name, in_table_name, sum_column, groupby_columns):
        """ select sum(`sum_column`), `other_columns` from table
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param sum_column: name of column over which sum is taken
        :param groupby_columns: names of columns to group by
        :return: sucess True/False
         """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.sumgroup(out_table_name, sum_column, groupby_columns)
        out_table.print()
        print("%d rows returned" % len(out_table.rows))
        self.__save_table(out_table_name, out_table)
        return True
    
    def countgroup(self, out_table_name, in_table_name, count_column, groupby_columns):
        """ select sum(`sum_column`), `other_columns` from table
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param count_column: name of column over which count is taken
        :param groupby_columns: names of columns to group by
        :return: sucess True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.countgroup(out_table_name, count_column, groupby_columns)
        out_table.print()
        print("%d rows returned" % len(out_table.rows))
        self.__save_table(out_table_name, out_table)
        return True

    def movavg(self, out_table_name, in_table_name, column, n):
        """ perform `n` item moving average over `column` of `table'
        :param out_table_name: name of the resulting table
        :param in_table_name: name of the input table
        :param column: name of the column
        :param n: number of items over which to take moving average
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("No table found")
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.movavg(out_table_name, column, n)
        out_table.print()
        self.__save_table(out_table_name, out_table)
        return True

    def movsum(self, out_table_name, in_table_name, column, n):
        """ perform `n` item moving sum over `column` of `table'
        :param out_table_name: name of the resulting table
        :param in_table_name: name of the input table
        :param column: name of the column
        :param n: number of items over which to take moving sum
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("No table found")
            return False
        in_table = self.__get_table(in_table_name)
        out_table = in_table.movsum(out_table_name, column, n)
        self.__save_table(out_table_name, out_table)
        out_table.print(num_rows=5)
        return True

    def count(self, out_table_name, in_table_name):
        """get count of rows in table
        :param out_table_name: name of the resulting table
        :param in_table_name: name of the input table
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False

        in_table = self.__get_table(in_table_name)
        out_table = in_table.count(out_table_name)
        self.__save_table(out_table_name, out_table)
        out_table.print()
        return True

    def avg(self, out_table_name, in_table_name, column):
        """ select avg(`column`) from `table`
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param column: name of the column to average over
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False

        in_table = self.__get_table(in_table_name)
        out_table = in_table.avg(out_table_name, column)
        self.__save_table(out_table_name, out_table)
        out_table.print()
        return True

    def sum(self, out_table_name, in_table_name, column):
        """ select avg(`column`) from `table`
        :param out_table_name: name of the output table
        :param in_table_name: name of the input table
        :param column: name of the column to average over
        :return: success True/False
        """
        if not self.__exists(in_table_name):
            print("Table %s not found" % in_table_name)
            return False

        in_table = self.__get_table(in_table_name)
        out_table = in_table.sum(out_table_name, column)
        self.__save_table(out_table_name, out_table)
        out_table.print()
        return True

    def Btree(self, table_name, column):
        """ create a Btree index on `table` based on `column`
        Note: all indexes will be based on 1 column
        :param table_name: name of the table
        :param column: name of the column to index
        :return: None
        """
        self.__get_table(table_name).btree_index(column)

    def Hash(self, table_name, column):
        """ create a Hash index on `table` based on `column`
        Note: all indexes will be based on 1 column
        :param table_name: name of the table
        :param column: name of the column to index
        :return: None
        """
        self.__get_table(table_name).hash_index(column)
