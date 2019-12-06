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

    class Criteria:
        """inner class to store criteria for joins and selects
        """
        def __init__(self, criteria_str, query_type):
            self.criteria_str=criteria_str
            self.query_type=query_type

            self.comparator_pattern = re.compile("=|!=|<|>|≥|≤")
            self.comparator_pattern_ne = re.compile("|!=|<|>|≥|≤")
            self.equijoin_comparator_pattern = re.compile("=")
            self.arithops_pattern ="[*/+\-\^]"
            self.logic_pattern = re.compile("and|or")
            
            self.comparators = []
            self.logic_operators = []
            self.num_conditions = 0   # number of conditions
            self.conditions = []
            self.arithops= []
            self.constants=[]
            
            #for joins only

            #constants
            self.eq_constants = []
            self.ne_constants = []
            #arithops
            self.eq_arithops = []
            self.ne_arithops = []
            #conditions
            self.eq_conditions = []
            self.ne_conditions = []
            #comparators
            self.eq_comparators = []
            self.ne_comparators = []

        def set_logic_operators(self):
            self.logic_operators = re.findall(self.logic_pattern, self.criteria_str)
            self.num_conditions = len(self.logic_operators)+1
   
        def get_arithop(self, criteria_str):
            """returns arithmetic operator or None if no arithmetic operator is found
            """
            arithop = re.findall(self.arithops_pattern,criteria_str)
            if len(arithop)>0:
                return arithop[0]
            else:
                return None 

        def set_conditions(self):
            conditions = re.split(self.logic_pattern, self.criteria_str)
            for i in range(0, self.num_conditions):
                arithop=self.get_arithop(str(conditions[i]))
                # expr = self.parse_expression(arithop, str(conditions[i]), i)
                self.parse_expression(arithop, str(conditions[i]), i)

            
            if (self.query_type=="join"):
                self.conditions = self.eq_conditions + self.ne_conditions
                self.arithops = self.eq_arithops + self.ne_arithops
                self.comparators = self.eq_comparators + self.ne_comparators
                self.constants = self.eq_constants + self.ne_constants


        def parse_expression(self, arithop, condition, i):
            condition = utils.remove_parentheses(condition)
            if self.query_type=="join":
                self.parse_join_expression(condition, i)
            else:
                self.parse_select_expression(condition, arithop, i)

        def parse_select_expression(self, condition, arithop, i):
            left, right = re.split(self.comparator_pattern, condition)
            comparator = re.findall(self.comparator_pattern, condition)[0]
            self.arithops.append(arithop)

            # determine if contant is on left or right side of comparator
            if utils.is_numeric(left):
                constant = left
                field = right
                # flip comparator
                self.comparators.append(utils.REVERSE_COMPARATOR[comparator])
            else:
                constant = right
                field=left
                self.comparators.append(comparator)

            if (arithop is not None):
                fields = field.split(arithop)
                tokenized_expr = [fields[0],constant,fields[1]]
            else:
                tokenized_expr=[field,constant]
            self.conditions.append(tokenized_expr)

        def parse_join_expression(self, condition, i):
            left, right = re.split(self.comparator_pattern, condition)

            # process left side of comparator
            t1_arithop = self.get_arithop(left)
            # check for arithmetic operators
            if (t1_arithop is None):
                t1 = utils.remove_parentheses(left.split(".")[0]).strip()
                t1_field = left.split(".")[1].strip()
                t1_constant=None
            else:
                left1,right1 = left.split(t1_arithop)
                if (utils.is_numeric(left1)):
                    t1_constant=utils.remove_parentheses(left1).strip()
                    t1_ = utils.remove_parentheses(right1).strip()
                    t1=t1_.split(".")[0]
                    t1_field=t1_.split(".")[1]
                else:
                    t1_constant=utils.remove_parentheses(right1).strip()
                    t1_ = utils.remove_parentheses(left1).strip()
                    t1=t1_.split(".")[0]
                    t1_field=t1_.split(".")[1]

            # process right side of comparator
            t2_arithop = self.get_arithop(right)
            if (t2_arithop is None):
                t2 = right.split(".")[0].strip()
                t2_field=utils.remove_parentheses(right.split(".")[1]).strip()
                t2_constant=None
            else:
                left2,right2 = right.split(t2_arithop)
                if (utils.is_numeric(left2)):
                    t2_constant = utils.remove_parentheses(left1).strip()
                    t2_ = utils.remove_parentheses(right1).strip()
                    t2=t2_.split(".")[0]
                    t2_field = t2.split(".")[1]
                else:
                    t2_constant=utils.remove_parentheses(right2).strip()
                    t2_=utils.remove_parentheses(left2).strip()
                    t2 = t2_.split(".")[0]
                    t2_field = t2_.split(".")[1]

            tokenized_expr = [t1, t1_field, t2, t2_field]
            if "=" in condition and "!=" not in condition:
                self.eq_conditions.append(tokenized_expr)
                self.eq_comparators.append("=")
                self.eq_arithops.append([t1_arithop,t2_arithop])
                self.eq_constants.append([t1_constant,t2_constant])
            else:
                self.ne_conditions.append(tokenized_expr)
                self.ne_comparators.append(re.findall(self.comparator_pattern, condition)[0])
                self.ne_arithops.append([t1_arithop,t2_arithop])
                self.ne_constants.append([t1_constant,t2_constant])

    def __init__(self, cmd, args):
        self.command = cmd
        self.args = args
        self.types = {
            self.Types.ONE_ARGS: ["inputfromfile", "count"],
            self.Types.TWO_ARGS: ["avg", "sum", "concat", "outputtofile", "Btree", "Hash"],
            self.Types.THREE_ARGS: ["movsum", "movavg"],
            self.Types.MULTI_WITHOUT_CRITERIA: ["project", "sumgroup", "avggroup", "countgroup", "sort"],
            self.Types.WITH_CRITERIA: ["select", "join"]
        }
        self.criteria = None

    def get_criteria(self, num_tables):
        """parses text, returns Criteria object
        """
        criteria = self.Criteria(str(self.args.split(",")[num_tables]), self.command)
        criteria.set_logic_operators()
        criteria.set_conditions()
        return criteria

    def get_columns(self, params, num_tables):
        columns = []
        columns_ = params.split("(")[1]
        for i in range(num_tables, len(columns_.split(","))):
            columns.append(columns_.split(",")[i].strip().replace(")", ""))
        return columns

    def get_tables(self, params, num_tables):
        tables = []
        tables_ = params.split("(")[1]
        for i in range(0, num_tables):
            tables.append(tables_.split(",")[i].strip().replace(")", ""))
        return tables

    def get_args(self):
        in_table = None
        columns = None
        criteria = None

        # has only 1 argument
        if self.command in self.types[self.Types.ONE_ARGS]:
            in_table=utils.remove_parentheses(self.args)
            return in_table, None, None

        # has two arguments, no need to worry about criteria
        elif self.command in self.types[self.Types.TWO_ARGS]:
            if self.command == "concat":
                num_tables = 2
                in_table = self.get_tables(self.args, num_tables)
            else:
                num_tables = 1
                in_table = self.get_tables(self.args, num_tables)
                columns = self.get_columns(self.args, num_tables)

            # parse for in_table, table2/column
            return in_table, columns, None

        # has 3 arguments
        elif self.command in self.types[self.Types.THREE_ARGS]:
            num_tables = 1
            in_table = self.get_tables(self.args, num_tables)
            columns = self.get_columns(self.args, num_tables)
            # parse for in_table, column, n
            return in_table, columns[0], columns[1]

        # has multiple arguments without criteria
        elif self.command in self.types[self.Types.MULTI_WITHOUT_CRITERIA]:
            # parse for in_table, columns list
            num_tables = 1
            in_table = self.get_tables(self.args, num_tables)
            columns = self.get_columns(self.args, num_tables)
            return in_table, columns, None

        # has criteria in
        if self.command in self.types[self.Types.WITH_CRITERIA]:
            # parse for in_table, columns, criteria
            if self.command == "join":
                num_tables = 2
                in_table = self.get_tables(self.args, num_tables)
            else:  # command is select
                num_tables = 1
                in_table = self.get_tables(self.args, num_tables)[0]

            criteria = self.get_criteria(num_tables)                
            return in_table, None, criteria

