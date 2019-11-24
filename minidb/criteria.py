import re

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
        
  