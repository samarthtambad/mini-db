from minidb.database import Database as mdb
from minidb.utils import Utils as utils
from minidb.criteria import ArgParser

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
        table_name, cmd, args = utils.parse(txt)
        # in_table, columns, criteria = ArgParser(cmd, args).get_args()

        # there were only comments in the input text
        if cmd is None:
            continue

        elif cmd == "inputfromfile":
            # arg_parser.get_args()
            db.input_from_file(table_name, "data/sales1")

        elif cmd == "outputtofile":
            db.output_to_file(table_name, "_tmp.txt")

        elif cmd == "select":
            # db.select()
            pass

        elif cmd == "project":
            # parse args
            orig_table_name = "R"
            columns = ["saleid", "qty", "pricerange"]
            db.project(table_name, orig_table_name, columns)

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


if __name__ == "__main__":
    start()
