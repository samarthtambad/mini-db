
class database:

    def __init__(self):
        pass

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
    
    def project(self):
        """

        """
        print("project()")
    
    def concat(self):
        """

        """
        print("concat()")
    
    def sort(self):
        """

        """
        print("sort()")
    
    def join(self):
        """

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