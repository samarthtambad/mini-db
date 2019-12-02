import re
import operator


OPERATORS = {
"<": operator.lt, ">": operator.gt, "=": operator.eq, "!=": operator.ne,
"≥":   operator.ge, "≤": operator.le, "and": operator.and_, "or": operator.or_, "*":operator.mul, "+":operator.add,
"/":operator.truediv,"-":operator.sub
}


# a class for all static helper functions
class Utils:

    REVERSE_COMPARATOR = {
    "<":">", "=":"=", "!=":"!=", ">":"<","≥":"≤","≥":"≤"
    }


    def __init__(self):
        pass

    @staticmethod
    def get_columns(params, num_tables):
        columns = []
        columns_ = params.split("(")[1]
        for i in range(num_tables, len(columns_.split(","))):
            columns.append(columns_.split(",")[i].strip().replace(")", ""))
        return columns

    @staticmethod
    def get_tables(params, num_tables):
        tables = []
        tables_ = params.split("(")[1]
        for i in range(0, num_tables):
            tables.append(tables_.split(",")[i].strip().replace(")", ""))
        return tables

    @staticmethod
    def parse(txt):
        txt = txt.replace(" ", "")  # remove all whitespaces
        txt = re.split("//", txt)[0]  # remove comments

        # if there were only comments in the input
        if len(txt) == 0:
            return None, None, None

        table_name = None
        mod_txt = txt.split(":=", 1)[0]     # no table assignment present
        if mod_txt != txt:
            table_name, txt = txt.split(":=", 1)  # extract table name
        cmd, _ = txt.split("(", 1)  # extract command
        args = txt.replace(cmd, "")  # remove command from text, only args remain

        # print(table_name, cmd, args)
        return table_name, cmd, args
