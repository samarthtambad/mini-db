import re


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
