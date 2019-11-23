# from BTrees.IIBTree import IIBTree
from database import Database as mdb
import re

"""TODO, Remove before submitting
Each operation will be on a single line. Each time you execute a line,
you should print the time it took to execute.
"""


def start():
    db = mdb()

    while True:
        txt = input("minidb>> ")
        cmd = txt.split("(")[0]

        if cmd == "inputfromfile":
            db.input_from_file("sales1")

        elif cmd == "outputtofile":
            db.output_to_file()
            
        elif cmd == "select":
            db.select()

        elif cmd == "project":
            db.project("table_name", "saleid", "itemid", "customerid", "storeid")

        elif cmd == "concat":
            db.concat()

        elif cmd == "sort":
            db.sort(table)

        elif cmd == "join":
            db.join()

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
            

if __name__ == "__main__":
    start()
