from minidb.database import Database as mdb

def tests(db):
    assert db.input_from_file("table1", "sales1") == True, "InputFromFile failed"

    assert db.project("table_not_present", "saleid", "itemid", "customerid", "storeid") == False
    assert db.project("table1", "saleid", "itemid", "customerid", "storeid") == True

    

if __name__ == "__main__":
    db = mdb()
    tests(db)
    print("All tests passed")