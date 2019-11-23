
from table import Table

class Database:

    def __init__(self):
        self.tables = []

    """TODO, Remove before submitting
    You will hand in clean and well structured source code in which each 
    function has a header that says: 
    (i) what the function deos, 
    (ii) what its inputs are and what they mean 
    (iii) what the outputs are and mean 
    (iv) any side effects to globals.
    """

    def inputFromFile(self, file):
        """Import data from given vertical bar delimited `file` into array-table. (1 or more columns)
        Parameters
        ----------
        `file` : string,
            path to the input file.
        Returns
        -------
        None
        """
        print("inputFromFile()")
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



    def outputToFile(self, table, file):
        """Output contents of `table` (with vertical bar separators) into `file`.
        Parameters
        ----------
        `table` : string,
            name of the table to output
            
        `file` : string
            path to the output file where output must be written.
        Returns
        -------
        None
        """
        print("outputToFile()")

    def select(self, table, criteria):
        """Select all columns from `table` satisfying the given `criteria`. Prints the result to standard output.
        Parameters
        ----------
        table : string,
            name of the table to output,
            
        `criteria` : string,
            condition(s) that each selected row must satisfy
        Returns
        -------
        None
        """
        print("select()")
    
    def project(self, table, *columns):
        """select a subset of columns from a table
        Parameters
        ----------
        `table` : string,
            name of the table from which to select columns,
            
        `columns` : string (multiple),
            columns to keep in the projection
        Returns
        -------
        None
        """
        print("project()")
    
    def concat(self, table1, table2):
        """concatenate two tables (with the same schema)
        Parameters
        ----------
        `table1` : string,
            name of the first table,
            
        `table2` : string,
            name of the second table
        Returns
        -------
        None
        """
        print("concat()")
    
    def sort(self, table, *columns):
        """sort `table` by each column in `columns` in the given order
        Parameters
        ----------
        `table` : string,
            name of the table,
            
        `columns` : string,
            name of the columns to sort by (in the given order)
        Returns
        -------
        None
        """
        print("sort()")
    
    def join(self, *tables, criteria):
        """select all columns from each of the `tables'. Filter rows by ones that satisfy the `criteria`
        Parameters
        ----------
        `tables` : string (multiple),
            names of the tables,
            
        `criteria` : string,
            condition(s) that each selected row must satisfy
        Returns
        -------
        None
        """
        print("join()")

    def avggroup(self):
        """
        
        """
        print("avggroup()")

    def sumgroup(self):
        """

        """
        print("sumgroup()")

    def movavg(self):
        """

        """
        print("movavg()")

    def movsum(self):
        """

        """
        print("movsum()")
    
    def avg(self):
        """

        """
        print("avg()")

    def Btree(self):
        """
        
        """
        print("Btree()")

    def Hash(self):
        """
        
        """
        print("Hash()")