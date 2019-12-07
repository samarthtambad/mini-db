import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def get_db():
    from minidb.database import Database
    db = Database()
    return db


@pytest.fixture
def get_argparser():
    from minidb.argparser import ArgParser
    return ArgParser


@pytest.fixture
def get_parser():
    from minidb.utils import Utils
    return Utils


def parse_assert(utils, txt, table_name, cmd, args):
    parsed_table_name, parsed_cmd, parsed_args = utils.parse(txt)
    assert parsed_table_name == table_name, "Error parsing table name"
    assert parsed_cmd == cmd, "Error parsing command"
    assert parsed_args == args, "Error parsing arguments"


def criteria_assert(parser, cmd, args, correct_conditions, correct_comparators, correct_arithops, correct_constants, correct_logic_operators):
    in_table, columns, criteria = parser(cmd, args).get_args()
    assert criteria.conditions == correct_conditions
    assert criteria.comparators == correct_comparators
    assert criteria.arithops == correct_arithops
    assert criteria.constants == correct_constants
    assert criteria.logic_operators == correct_logic_operators


def join_assert(count, correct_count):
    assert count == correct_count


def test(get_db):
    db = get_db
    # test taking input from file
    assert db.input_from_file("table1", "data/sales1") is True, "File not found"
    assert len(db.tables) == 1, "Created table not saved in database"

    # test projection function
    assert db.project("projected_table", "table_not_present", ["saleid", "itemid", "customerid", "storeid"]) \
        is False, "Should have thrown table not found error"
    assert db.project("projected_table", "table1", ["column_not_present"]) is False, \
        "Should have thrown column not found error"
    assert db.project("projected_table", "table1", ["saleid", "itemid", "customerid", "storeid"]) \
        is True, "Error in db.project()"
    assert len(db.tables) == 2, "Projected table not saved in database"


def test_index(get_db):
    db = get_db
    db.input_from_file("X", "data/sales1")
    db.input_from_file("Y", "data/sales2")
    x = db.tables["X"]
    y = db.tables["Y"]
    db.Hash("X", "qty")
    assert len(x.indexes) == 1, "Index on qty not added properly (sales1)"
    db.Hash("X", "pricerange")
    assert len(x.indexes) == 2, "Index on pricerange not added properly (sales1)"
    db.Btree("Y", "saleid")
    assert len(y.indexes) == 1, "Index on saleid not added properly (sales2)"
    db.Btree("Y", "P")
    assert len(y.indexes) == 2, "Index on P not added properly (sales2)"
    unique_qty = set()
    for r in x.rows:
        unique_qty.add(r[x.col_names["qty"]])
    assert len(x.indexes["qty"].index) == len(unique_qty)
    unique_saleid = set()
    for r in y.rows:
        unique_saleid.add(r[y.col_names["saleid"]])
    assert len(y.indexes["saleid"].index) == len(unique_saleid)


def test_join(get_db, get_parser, get_argparser):
    db = get_db
    argparser = get_argparser
    utils = get_parser
    db.input_from_file("A", "data/sales1")
    db.input_from_file("B", "data/sales2")
    db.input_from_file("A_small", "data/sales1_small")
    db.input_from_file("B_small", "data/sales2_small")

    queries = ["E3:=join(A,B,(A.qty=3*B.Q))", "E3:=join(A,B,(A.saleid-36=B.saleid))",
               "E3:=join(A_small,B_small,(A_small.qty≤B_small.Q)", "E3:=join(A,B,(A.qty≤B.Q) and (A.saleid=B.saleid))",
               "E1:=join(A,B,(A.saleid=B.saleid) and (A.pricerange!=B.P))", "E1:=join(A,B,(3*B.Q=A.qty"]
    results = [598839, 964, 51, 539, 868, 598839]
    for i in range(0, len(queries)):
        table_name, cmd, args = utils.parse(queries[i])
        in_table, columns, criteria = argparser(cmd, args).get_args()
        assert db.join(table_name, in_table, criteria) is True, ""
        t = db.tables[table_name]
        join_assert(t.num_rows, results[i])


# tests for detecting parsing errors
def test_parsing(get_parser):
    utils = get_parser
    parse_assert(utils, "// parser handling comments test", None, None, None)
    parse_assert(utils, "R := inputfromfile(sales1) // import vertical bar delimited foo, first line",
                 "R", "inputfromfile", "(sales1)")
    parse_assert(utils, "R1 := select(R, (time > 50) or (qty < 30))", "R1", "select", "(R,(time>50)or(qty<30))")
    parse_assert(utils, "R2 := project(R1, saleid, qty, pricerange) // select saleid, qty, pricerange",
                 "R2", "project", "(R1,saleid,qty,pricerange)")
    parse_assert(utils, "R3 := avg(R1, qty) // select avg(qty) from R1", "R3", "avg", "(R1,qty)")
    parse_assert(utils, "R4 := sumgroup(R1, time, qty) // select sum(time), qty from R1 group by qty",
                 "R4", "sumgroup", "(R1,time,qty)")
    parse_assert(utils, "R5 := sumgroup(R1, qty, time, pricerange) // select sum(qty), time,", "R5",
                 "sumgroup", "(R1,qty,time,pricerange)")
    parse_assert(utils, "T := join(R, S, R.customerid = S.C) // select * from R, S", "T",
                 "join", "(R,S,R.customerid=S.C)")
    parse_assert(utils, "T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid)) // select * from R1, S",
                 "T1", "join", "(R1,S,(R1.qty>S.Q)and(R1.saleid=S.saleid))")
    parse_assert(utils, "outputtofile(Q5, Q5)", None, "outputtofile", "(Q5,Q5)")
    parse_assert(utils, "Hash(R,itemid)", None, "Hash", "(R,itemid)")
    parse_assert(utils, "R1:= select(t1, (time > 50))", "R1", "select", "(t1,(time>50))")


def test_criteria_parsing(get_argparser):
    # correct_conditions, correct_comparators, correct_arithops, correct_constants, correct_logic_operators):
    parser = get_argparser
    # select test cases
    criteria_assert(parser, "select", "(t1,(time=50))", [["time", "50"]], ["="], [None], [], [])
    # test with spaces
    criteria_assert(parser, "select", "(t1,(time = 50))", [["time", "50"]], ["="], [None], [], [])
    # test with multiple conditions
    criteria_assert(parser, "select", "(t1,(time = 50) or (time > 100))", [["time", "50"], ["time", "100"]], ["=", ">"],
                    [None, None], [], ["or"])

    # test all arithmetic operators
    criteria_assert(parser, "select", "(t1,(time*2=50))", [["time", "50", "2"]], ["="], ["*"], [], [])
    criteria_assert(parser, "select", "(t1,(time/2=50))", [["time", "50", "2"]], ["="], ["/"], [], [])

    # # join test cases
    # # test single conditions
    criteria_assert(parser, "join", "(t1,t2,(t1.time+10>t2.T))", [["t1", "time", "t2", "T"]], [">"], [["+", None]],
                    [["10", None]], [])

    # # test multiple conditions
    criteria_assert(parser, "join", "(t1,t2,(t1.time+10>t2.T+10) and (t1.saleid=t2.saleid))",
                    [["t1", "saleid", "t2", "saleid"], ["t1", "time", "t2", "T"]], ["=", ">"],
                    [[None, None], ["+", "+"]], [[None, None], ["10", "10"]], ["and"])

    criteria_assert(parser, "join", "(t1,t2, ( t1.time + 10 > t2.T+10 ) and  (t1.saleid=t2.saleid))",
                    [["t1", "saleid", "t2", "saleid"], ["t1", "time", "t2", "T"]], ["=", ">"],
                    [[None, None], ["+", "+"]], [[None, None], ["10", "10"]], ["and"])
