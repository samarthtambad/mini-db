import time
from minidb.argparser import ArgParser
from minidb.database import Database as mdb
from minidb.utils import Utils as utils

data_path = "data/"


def start():
    db = mdb()

    while True:
        txt = input("\nminidb>> ")

        start_time = time.time()

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

        elif cmd=="count":
            db.count(table_name, in_table[0])

        elif cmd == "inputfromfile":
            db.input_from_file(table_name, data_path + in_table)

        elif cmd == "outputtofile":
            db.output_to_file(in_table[0], columns[0])

        elif cmd == "select":
            db.select(table_name, in_table[0], criteria)
            pass

        elif cmd == "project":
            db.project(table_name, in_table[0], columns)

        elif cmd == "concat":
            db.concat(table_name, in_table)

        elif cmd == "sort":
            db.sort(table_name, in_table[0], columns)

        elif cmd == "join":
            db.join(table_name, in_table, criteria)

        elif cmd == "avggroup":
            db.avggroup(table_name, in_table[0], columns[0], columns[1:])

        elif cmd == "sumgroup":
            db.sumgroup(table_name, in_table[0], columns[0], columns[1:])

        elif cmd == "movavg":
            n = int(criteria)
            db.movavg(table_name, in_table[0], columns, n)

        elif cmd == "movsum":
            n = int(criteria)
            db.movsum(table_name, in_table[0], columns, n)

        elif cmd == "avg":
            db.avg(table_name, in_table[0], columns[0])

        elif cmd=="sum":
            db.sum(table_name, in_table[0], columns[0])

        elif cmd == "Btree":
            db.Btree(in_table[0], columns[0])

        elif cmd == "Hash":
            db.Hash(in_table[0], columns[0])

        else:  # default
            print("Wrong command. Use help to find out the correct usage")

        end_time = time.time()
        print("\nTime taken: %0.5f s" % (end_time - start_time))


if __name__ == "__main__":
    start()
