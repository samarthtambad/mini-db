import re
from enum import Enum


class ArgParser:

    class Types(Enum):
        ONE_ARGS = 0
        TWO_ARGS = 1
        THREE_ARGS = 2
        MULTI_WITHOUT_CRITERIA = 3
        WITH_CRITERIA = 4

    # TODO: What functions should this support?
    class Criteria:

        def __init__(self):
            # self.criteria = criteria
            self.comparator_pattern = re.compile("=|!=|<|>|≥|≤")
            pass

    def __init__(self, cmd, args):
        self.command = cmd
        self.args = args
        self.types = {
            self.Types.ONE_ARGS: ["inputfromfile"],
            self.Types.TWO_ARGS: ["avg", "concat", "outputtofile", "Btree", "Hash"],
            self.Types.THREE_ARGS: ["movsum", "movavg"],
            self.Types.MULTI_WITHOUT_CRITERIA: ["project", "sumgroup", "avggroup"],
            self.Types.WITH_CRITERIA: ["select", "join"]
        }
        self.criteria=None

    def get_criteria(self, txt):
        """parses text, returns Criteria object
        """
        pass

    def get_args(self):
        in_table = None
        columns = None
        criteria = None

        # TODO:

        # has only 1 argument
        if self.command in self.types[self.Types.ONE_ARGS]:
            # parse for in_table
            return in_table, None, None

        # has two arguments, no need to worry about criteria
        elif self.command in self.types[self.Types.TWO_ARGS]:
            # parse for in_table, table2/column
            return in_table, columns, None

        # has 3 arguments
        elif self.command in self.types[self.Types.THREE_ARGS]:
            n = None
            # parse for in_table, column, n
            return in_table, columns, n

        # has multiple arguments without criteria
        elif self.command in self.types[self.Types.MULTI_WITHOUT_CRITERIA]:
            # parse for in_table, columns list
            return in_table, columns, None

        # has criteria in
        if self.command in self.types[self.Types.WITH_CRITERIA]:
            self.criteria=self.Criteria()
            if (self.command=="join"):
                a,b=re.split(self.criteria.comparator_pattern,self.args.split(",")[2])
                t1=a.split(".")[0].strip()
                t1_field=a.split(".")[1].strip()
                t2=b.split(".")[0].strip()
                t2_field=b.split(".")[1].strip()
                in_table=[t1,t2]
            # else: #command is select
                # do something


            # parse for in_table, columns, criteria
            # criteria = self.Criteria("some criteria placeholder")
            return in_table, columns, criteria


# class Criteria():
#     def __init__(self, criteria_str):
#         comparators = "[=<>!=≥≤]"
#
#         a,b=re.split(comparators,criteria_str)
#         self.t1=a.split(".")[0].strip()
#         self.t1_field=a.split(".")[1].strip()
#
#         self.t2=b.split(".")[0].strip()
#         self.t2_field=b.split(".")[1].strip()
#
#     def __str__(self):
#         return "table 1: %s, field: %s\ntable 2: %s, field: %s" % (self.t1, self.t1_field,self.t2, self.t2_field)

# parse criteria
