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
            self.arithops_pattern ="[*/+\-\^]"

            self.logic_pattern = re.compile("and|or")
            self.comparators = None
            self.logic_operators = None
            self.num_conditions = 0   # number of conditions
            self.conditions = None  # conditions will be implemented as a list when present so that it is stored contiguously for faster access
            
            self.arithops=[]
            self.constants=[]


        def get_arithops(self, criteria_str):
            # return True
            return re.findall(self.arithops_pattern,criteria_str)


    def __init__(self, cmd, args):
        self.command = cmd
        self.args = args
        self.types = {
            self.Types.ONE_ARGS: ["inputfromfile", "count"],
            self.Types.TWO_ARGS: ["avg", "sum", "concat", "outputtofile", "Btree", "Hash"],
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
                columns = utils.get_columns(self.args, num_tables)
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

            if self.command == "join":
                num_tables = 2
            else:  # command is select
                num_tables = 1

            in_table = utils.get_tables(self.args, num_tables)
            criteria_str = str(self.args.split(",")[num_tables])

            criteria = self.Criteria()
            # check if there are arithmetic operations


            criteria.conditions = [] #initialize criteria conditions to an empty list
            
            # get comparators
            criteria.comparators = re.findall(criteria.comparator_pattern, self.args)
            # get number of criteria
            criteria.num_conditions = len(criteria.comparators)
            conditions = re.split(criteria.logic_pattern, criteria_str)

            # get logic operators
            criteria.logic_operators = re.findall(criteria.logic_pattern, criteria_str)
            # print(self.criteria.logic_operators)

            
            for i in range(0, criteria.num_conditions):
                arithops=criteria.get_arithops(str(conditions[i]))
                if len(arithops)>0:
                    arithop=arithops[0]
                else:
                    arithop=None
                
                criteria.arithops.append(arithop)
                ex = self.parse_expression(criteria.comparator_pattern,arithop,str(conditions[i]), num_tables,i,criteria)
                criteria.conditions.append(ex)
            # print(self.criteria.conditions)

            return in_table, columns, criteria

    def is_numeric(self, x):
        try:
            float(x)
            return True
        except ValueError:
            return False

    def parse_expression(self,comparator_pattern, arithop, params, num_tables,i,criteria):
        tokenized_expr = []
        left, right = re.split(comparator_pattern, params.replace(")","").replace("(",""))

        if num_tables > 1:
            return self.parse_join_expression(left,right,arithop,i,criteria)

        # command is select
        else:
            return self.parse_select_expression(left,right,arithop,i,criteria)

    def parse_select_expression(self,left,right,arithop,i,criteria):
        # determine if contant is on left or right side of comparator
        if self.is_numeric(left):
            constant = left
            field = right
            # flip comparator
            criteria.comparators[i]=utils.REVERSE_COMPARATOR[criteria.comparators[i]]
        else:
            constant = right
            field=left

        if (arithop is not None):
            fields = field.split(arithop)
            tokenized_expr = [fields[0],constant,fields[1]]
        else:
            tokenized_expr=[field,constant]
        return tokenized_expr

    def parse_join_expression(self,left,right):
        t1 = left.split(".")[0].replace("(", "").replace(")", "").strip()
        t1_field = left.split(".")[1].strip()
        t2 = right.split(".")[0].strip()
        t2_field = right.split(".")[1].replace("(", "").replace(")", "").strip()
        tokenized_expr = [t1, t1_field, t2, t2_field]
