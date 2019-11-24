from minidb.database import Database as mdb

def tests(db):
    assert db.input_from_file("table1", "sales1") == True, "File not found"

    assert db.project("table_not_present", ["saleid", "itemid", "customerid", "storeid"]) == False, "Should have thrown table not found error"
    assert db.project("table1", ["saleid", "itemid", "customerid", "storeid"]) == True, "Error in db.project()"
    assert db.project("table1", ["column_not_present"]) == False, "Should have thrown column not found error"

if __name__ == "__main__":
    db = mdb()
    tests(db)
    print("All tests passed")