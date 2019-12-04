import re
import operator

# a class for all static helper functions
class Utils:

    # lookups
    OPERATORS = {
    "<": operator.lt, ">": operator.gt, "=": operator.eq, "!=": operator.ne,
    "≥":   operator.ge, "≤": operator.le, "and": operator.and_, "or": operator.or_, "*":operator.mul, "+":operator.add,
    "/":operator.truediv,"-":operator.sub
    }

    NUMERIC = {
        operator.lt: True, operator.gt: True, operator.eq: False, operator.ne: False,
        operator.ge: True, operator.le: True, operator.and_: False, operator.or_: False
    }

    REVERSE_COMPARATOR = {
        "<":">",
        "=":"=",
        "!=":"!=",
        ">":"<",
        "≥":"≤",
        "≤":"≥"
    }

    COMPARATOR_VALUES = {
    "<":">", "=":"1", "!=":"!=", ">":"<","≥":"≤","≥":"≤"
    }

    def __init__(self):
        pass

    @staticmethod
    def is_numeric(x):
        try:
            float(x)
            return True
        except ValueError:
            return False

    @staticmethod
    def remove_parentheses(params):
        return params.replace(")","").replace("(","")

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
