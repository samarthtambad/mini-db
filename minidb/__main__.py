# from BTrees.IIBTree import IIBTree
from minidb.database import Database as mdb
import re

"""TODO, Remove before submitting
Each operation will be on a single line. Each time you execute a line,
you should print the time it took to execute.
"""


def start():
    db = mdb()

    while True:
        txt = input("minidb>> ")

        output, cmd, params = parse_instr(txt)

        if cmd == "inputfromfile":
            table_name = "R"
            db.input_from_file(table_name, "sales1")

        elif cmd == "outputtofile":
            table_name = "R"
            db.output_to_file(table_name, "_tmp.txt")

        elif cmd == "select":
            db.select()

        elif cmd == "project":
            table_name = "R"
            db.project(table_name, "saleid", "itemid", "customerid", "storeid")

        elif cmd == "concat":
            tables = get_tables(params, 2)
            db.concat(tables)

        elif cmd == "sort":
            table = get_tables(params, 1)
            db.sort(table)


        elif cmd == "join":
            tables = get_tables(params, 2)
            criteria = get_criteria(params)
            db.join(tables, criteria)

        elif cmd == "avggroup":
            db.avggroup()

        elif cmd == "sumgroup":
            db.sumgroup()

        elif cmd == "movavg":
            db.movavg()

        elif cmd == "movsum":
            db.movsum()

        elif cmd == "avg":
            db.avg()

        elif cmd == "Btree":
            db.btree()

        elif cmd == "Hash":
            db.hash()

        elif cmd == "exit":
            break

        else:  # default
            print("Wrong command. Use help to find out the correct usage")


def parse_instr(txt):
    txt = re.split("//", txt)[0]
    output, instr = re.split("(:=)", txt)[0].strip(), re.split("(:=)", txt)[2]
    cmd = instr.split("(")[0]
    params = instr[len(cmd):]
    return output.strip(), cmd.strip(), params.strip()


def get_criteria(params):
    criteria = []
    re.split(",", params)[2]
    # split on and,or to get list of commands
    # TODO: loop through criteria using regexp and create new criteria objects for each string between "(" ")"
    criteria_object = Criteria(...)
    criteria.append(criteria_object)


def get_tables(params, num_tables):
    tables = []
    tables_ = params.split("(")[1]
    for i in range(0, num_tables):
        tables.append(tables_.split(",")[i].strip().replace(")", ""))
    return tables


if __name__ == "__main__":
    start()
