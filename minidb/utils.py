import re


# a class for all static helper functions
class Utils:

    def __init__(self):
        pass

    @staticmethod
    def parse_expression(pattern, params, num_tables):
        tokenized_expr = []
        a, b = re.split(pattern, params)
        if num_tables > 1:
            t1 = a.split(".")[0].replace("(", "").replace(")", "").strip()
            t1_field = a.split(".")[1].strip()
            t2 = b.split(".")[0].strip()
            t2_field = b.split(".")[1].replace("(", "").replace(")", "").strip()
            tokenized_expr = [t1, t1_field, t2, t2_field]
        else:
            t1_field = a.replace("(", "").replace(")", "")
            t2_field = b.replace("(", "").replace(")", "")
            tokenized_expr = [t1_field, t2_field]
        return tokenized_expr

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
