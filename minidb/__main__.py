from minidb.database import Database as mdb
from minidb.utils import Utils as utils
from minidb.argparser import ArgParser

"""TODO, Remove before submitting
Each operation will be on a single line. Each time you execute a line,
you should print the time it took to execute.
"""

data_path = "data/"


"""
Issues:
1. ArgParser returns columns = [] for 'project' command.
2. ArgParser in_table is always None
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

        elif txt == "show_index":
            db.show_index()
            continue

        # handle other commands after parsing
        table_name, cmd, args = utils.parse(txt)
        in_table, columns, criteria = ArgParser(cmd, args).get_args()

        # there were only comments in the input text
        if cmd is None:
            continue

        elif cmd == "inputfromfile":
            # print(table_name, cmd, in_table)
            db.input_from_file(table_name, data_path + in_table)

        elif cmd == "outputtofile":
            db.output_to_file(table_name, "_tmp.txt")

        elif cmd == "select":
            db.select(table_name, in_table[0], criteria)
            pass

        elif cmd == "project":
            # parse args
            columns = ["saleid", "qty", "pricerange"]
            # print(table_name, cmd, in_table[0], columns)
            db.project(table_name, in_table[0], columns)

        elif cmd == "concat":
            db.concat(table_name, in_table)

        elif cmd == "sort":
            db.sort(table_name, in_table[0], columns)

        elif cmd == "join":
            db.join(table_name, in_table, criteria)
            pass

        elif cmd == "avggroup":
            db.avggroup(table_name,in_table[0],columns[0],columns[1:])
            pass

        elif cmd == "sumgroup":
            db.sumgroup(table_name,in_table[0],columns[0],columns[1:])
            pass

        elif cmd == "movavg":
            in_table = "T1"
            column = "qty"
            n = 3
            print(table_name, cmd, in_table, column, n)
            db.movavg(table_name, in_table, column, n)
            pass

        elif cmd == "movsum":
            in_table = "T1"
            column = "qty"
            n = 3
            print(table_name, cmd, in_table, column, n)
            db.movsum(table_name, in_table, column, n)
            pass

        elif cmd == "avg":
            db.avg(table_name,in_table[0],columns)
            pass

        elif cmd == "Btree":
            in_table = "T1"
            column = "qty"
            db.Btree(in_table, column)

        elif cmd == "Hash":
            in_table = "T1"
            column = "qty"
            db.Hash(in_table, column)

        else:  # default
            print("Wrong command. Use help to find out the correct usage")


if __name__ == "__main__":
    start()
