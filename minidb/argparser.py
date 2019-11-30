import re
from enum import Enum
from minidb.utils import Utils as utils


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
            self.logic_pattern = re.compile("and|or")
            self.comparators = None
            self.logic_operators = None
            self.num_conditions = 0   # number of conditions
            self.conditions = None  # conditions will be implemented as a list when present so that it is stored contiguously for faster access

        def join_to_select(self):
            select_conditions = []
            for i in range(0,self.num_conditions):
                c1 = self.conditions[i][0] + "_" + self.conditions[i][1]
                c2 = self.conditions[i][2] + "_" + self.conditions[i][3]
                select_conditions.append([c1, c2])
            self.conditions = select_conditions

    def __init__(self, cmd, args):
        self.command = cmd
        self.args = args
        self.types = {
            self.Types.ONE_ARGS: ["inputfromfile"],
            self.Types.TWO_ARGS: ["avg", "concat", "outputtofile", "Btree", "Hash"],
            self.Types.THREE_ARGS: ["movsum", "movavg"],
            self.Types.MULTI_WITHOUT_CRITERIA: ["project", "sumgroup", "avggroup","sort"],
            self.Types.WITH_CRITERIA: ["select", "join"]
        }
        self.criteria = None

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
            in_table = self.args.replace("(", "").replace(")", "")
            return in_table, None, None

        # has two arguments, no need to worry about criteria
        elif self.command in self.types[self.Types.TWO_ARGS]:
            if self.command == "concat":
                num_tables = 2
                in_table = utils.get_tables(self.args, num_tables)
            else:
                num_tables = 1
                in_table = utils.get_tables(self.args, num_tables)
                columns = utils.get_columns(self.args, num_tables)

            # parse for in_table, table2/column
            return in_table, columns, None

        # has 3 arguments
        elif self.command in self.types[self.Types.THREE_ARGS]:
            num_tables = 1
            in_table = utils.get_tables(self.args, num_tables)
            columns = utils.get_columns(self.args, num_tables)
            # parse for in_table, column, n
            return in_table, columns[0], columns[1]

        # has multiple arguments without criteria
        elif self.command in self.types[self.Types.MULTI_WITHOUT_CRITERIA]:
            # parse for in_table, columns list
            if self.command == "project":
                num_tables = 1
                in_table = utils.get_tables(self.args, num_tables)
                columns = utils.get_tables(self.args, num_tables)
            elif self.command == "sort":
                num_tables = 1
                in_table = utils.get_tables(self.args, num_tables)
                columns = utils.get_columns(self.args, num_tables)
            elif self.command == "sumgroup" or self.command == "avggroup":
                num_tables = 1
                in_table = utils.get_tables(self.args, num_tables)
                # first column will be what is summed or grouped
                columns = utils.get_columns(self.args, num_tables)

            return in_table, columns, None


        # has criteria in
        if self.command in self.types[self.Types.WITH_CRITERIA]:
            # parse for in_table, columns, criteria

            criteria = self.Criteria()
            criteria.conditions = [] #initialize criteria conditions to an exmpty list
            criteria.comparators = re.findall(criteria.comparator_pattern, self.args)
            criteria.num_conditions = len(criteria.comparators)

            if self.command == "join":
                num_tables = 2
            else:  # command is select
                num_tables = 1

            in_table = utils.get_tables(self.args, num_tables)
            criteria_str = str(self.args.split(",")[num_tables])
            
            criteria.logic_operators = re.findall(criteria.logic_pattern, criteria_str)
            # print(self.criteria.logic_operators)
            conditions = re.split(criteria.logic_pattern, criteria_str)
            
            for i in range(0, criteria.num_conditions):
                criteria.conditions.append(utils.parse_expression(criteria.comparator_pattern, str(conditions[i]), num_tables))
            # print(self.criteria.conditions)

            return in_table, columns, criteria
