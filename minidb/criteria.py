import re

<<<<<<< HEAD
class Criteria():
    def __init__(self, criteria_str):
        comparators = "[=<>!=≥≤]"

        a,b=re.split(comparators,criteria_str)
        self.t1=a.split(".")[0].strip()
        self.t1_field=a.split(".")[1].strip()
        
        self.t2=b.split(".")[0].strip()
        self.t2_field=b.split(".")[1].strip()
        
    def __str__(self):
        return "table 1: %s, field: %s\ntable 2: %s, field: %s" % (self.t1, self.t1_field,self.t2, self.t2_field)
        
  
=======

class Criteria:

    def __init__(self, criteria_str):
        comparators = "[=<>!=≥≤]"
        self.table1_name = None
        self.table1_field = None
        self.table2 = None
        self.comparator = None
        self.table1_name = criteria_str.split(".")[0]
        table1_field = criteria_str.split(",")[1]
        self.table1_field = filter(None, re.split(comparators, table1_field))
>>>>>>> master
