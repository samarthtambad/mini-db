import re


# a class for all static helper functions
class Utils:

    def __init__(self):
        pass

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

    # def parse_instr(txt):
    #     # first catch special commands
    #     if txt == "exit":
    #         return None, "exit", None
    #     elif txt == "show tables":
    #         return None, "show tables", None
    #     else:
    #         try:
    #             txt = re.split("//", txt)[0]
    #             output, instr = re.split("(:=)", txt)[0].strip(), re.split("(:=)", txt)[2]
    #             cmd = instr.split("(")[0]
    #             params = instr[len(cmd):]
    #             return output.strip(), cmd.strip(), params.strip()
    #         except:
    #             return "incorrect", None, None

    # def get_criteria(params):
    #     criteria = []
    #     re.split(",", params)[2]
    #     # split on and,or to get list of commands
    #     # TODO: loop through criteria using regexp and create new criteria objects for each string between "(" ")"
