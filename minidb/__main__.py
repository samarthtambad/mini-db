# from BTrees.IIBTree import IIBTree
from minidb.database import Database as mdb
from minidb.criteria import Criteria
import re

"""TODO, Remove before submitting
Each operation will be on a single line. Each time you execute a line,
you should print the time it took to execute.
"""


def start():
    db = mdb()

    while True:
        txt = input("\n\nminidb>> ")

        # handle special commands which don't require further parsing
        if txt == "exit":
            break

        elif txt == "show_tables":
            db.show_tables()
            continue

        # handle other commands after parsing
        table_name, cmd, args = parse(txt)

        # there were only comments in the input text
        if cmd is None:
            continue

        elif cmd == "inputfromfile":
            db.input_from_file(table_name, "data/sales1")

        elif cmd == "outputtofile":
            db.output_to_file(table_name, "_tmp.txt")

        elif cmd == "select":
            # db.select()
            pass

        elif cmd == "project":
            # parse args
            db.project(table_name, ["saleid", "itemid", "customerid", "storeid"])

        elif cmd == "concat":
            # parse args
            # tables = get_tables(params, 2)
            # db.concat(tables)
            pass

        elif cmd == "sort":
            # table = get_tables(params, 1)
            # db.sort(table)
            pass

        elif cmd == "join":
            # tables = get_tables(params, 2)
            # criteria = get_criteria(params)
            # db.join(output,tables, criteria)
            pass

        elif cmd == "avggroup":
            # db.avggroup()
            pass

        elif cmd == "sumgroup":
            # db.sumgroup()
            pass

        elif cmd == "movavg":
            # db.movavg()
            pass

        elif cmd == "movsum":
            # db.movsum()
            pass

        elif cmd == "avg":
            # db.avg()
            pass

        elif cmd == "Btree":
            # db.btree()
            pass

        elif cmd == "Hash":
            # db.hash()
            pass

        else:  # default
            print("Wrong command. Use help to find out the correct usage")


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


def parse(txt):
    txt = txt.replace(" ", "")      # remove all whitespaces
    txt = re.split("//", txt)[0]    # remove comments

    # if there were only comments in the input
    if len(txt) == 0:
        return None, None, None

    table_name, txt = txt.split(":=", 1)    # extract table name
    cmd, _ = txt.split("(", 1)              # extract command
    args = txt.replace(cmd, "")         # remove command from text, only args remain

    # print(table_name, cmd, args)
    return table_name, cmd, args


# def get_criteria(params):
#     criteria = []
#     re.split(",", params)[2]
#     # split on and,or to get list of commands
#     # TODO: loop through criteria using regexp and create new criteria objects for each string between "(" ")"


def get_tables(params, num_tables):
    tables = []
    tables_ = params.split("(")[1]
    for i in range(0, num_tables):
        tables.append(tables_.split(",")[i].strip().replace(")", ""))
    return tables


if __name__ == "__main__":
    start()
