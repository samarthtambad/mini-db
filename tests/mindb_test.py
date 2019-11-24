import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def get_db():
    from minidb.database import Database
    db = Database()
    return db


def test(get_db):
    db = get_db
    assert db.input_from_file("table1", "data/sales1") is True, "File not found"

    assert db.project("table_not_present", ["saleid", "itemid", "customerid", "storeid"]) is False, "Should have thrown table not found error"
    assert db.project("table1", ["saleid", "itemid", "customerid", "storeid"]) is True, "Error in db.project()"
    assert db.project("table1", ["column_not_present"]) is False, "Should have thrown column not found error"

    print("All tests passed")

