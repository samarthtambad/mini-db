from minidb.table_nparr import Table
import operator


OPERATORS = {
    "<": operator.lt, ">": operator.gt, "=": operator.eq, "!=": operator.ne,
    "≥":   operator.ge, "≤": operator.le, "and": operator.and_, "or": operator.or_
}


class MergeJoin:
	def __init__(self, t1, t2, criteria):
		self.t1=t1
		self.t2=t2
		self.conditions=criteria.conditions
		self.out_rows=[]
		self.comparators=criteria.comparators


	def join(self):
		self.sort_tables()

		t1_max=self.t1.num_rows
		t2_max=self.t2.num_rows

		t1_col = self.conditions[0][1]
		t2_col = self.conditions[0][3]

		op = OPERATORS[self.comparators[0]]

		if (op==operator.eq):
			return self.join_equal(0,0,t1_max,t2_max,t1_col,t2_col)
		elif (op==operator.gt):
			return self.join_gt(0,0,t1_max,t2_max,t1_col,t2_col)
		elif (op==operator.ne):
			return self.join_ne(0,0,t1_max,t2_max,t1_col,t2_col)


	def join_gt(self, t1_row, t2_row, t1_max, t2_max, t1_col, t2_col):
		# print(t1_col)
		# print(t2_col)
		while (t1_row < t1_max and t2_row < t2_max):

			t1_val = float(self.t1.get_value(t1_row, t1_col))
			t2_val = float(self.t2.get_value(t2_row, t2_col))

			# print(t1_row)
			# print(t2_row)

			# print("t1 val: " + str(t1_val))
			# print("t2 val: " + str(t2_val))

			if (t1_val == t2_val):
				t1_row+=1
				t2_row=0
			elif (t1_val < t2_val):
				t1_row+=1
			else:
				new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
				self.out_rows.append(new_row)
				self.join_gt_(t1_row,t2_row)
				t2_row+=1

	def join_gt_(self,t1_row,t2_row):
		for i in range(0,t2_row):
			new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(i).tolist()
			self.out_rows.append(new_row)



			



		


		# for t1_row, row1 in enumerate(self.t1.rows):
		# 	for t2_row, row2 in enumerate(self.t2.rows):
		# 		t1_val = float(self.t1.get_value(t1_row, t1_col))
		# 		t2_val = float(self.t2.get_value(t2_row, t2_col))

		# 		if (t1_val > t2_val):
		# 			new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
		# 			self.out_rows.append(new_row)
					
		# 		elif (t1_val == t2_val):
		# 			continue
		# 		elif (t1_val < t2_val):
		# 			continue


		return self.out_rows



	def join_equal(self, t1_row, t2_row, t1_max, t2_max, t1_col, t2_col):
		
		while (t1_row < t1_max and t2_row < t2_max):
			t1_val = self.t1.get_value(t1_row, t1_col) 
			t2_val = self.t2.get_value(t2_row, t2_col)

			if (t1_val == t2_val):
				new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
				self.out_rows.append(new_row)
				t2_row+=1
			elif (t1_val<t2_val):
				t1_row+=1
			else:
				t2_row+=1

		return self.out_rows


	def join_ne(self, t1_row, t2_row, t1_max, t2_max, t1_col, t2_col):
		
		while (t1_row < t1_max and t2_row < t2_max):
			t1_val = self.t1.get_value(t1_row, t1_col) 
			t2_val = self.t2.get_value(t2_row, t2_col)

			if (t1_val == t2_val):
				t2_row+=1
			elif (t1_val<t2_val):
				new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
				self.out_rows.append(new_row)
				t1_row+=1
			else:
				new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
				self.out_rows.append(new_row)
				t2_row+=1

		return self.out_rows
 

	def sort_tables(self):
		# sort tables on columns to join on (using first join condition)
		if (len(self.conditions)>0):
			self.t1.rows=self.t1.sort(None, [self.conditions[0][1]])
			self.t2.rows=self.t2.sort(None, [self.conditions[0][3]])

			# self.t1.print_formatted()
			# self.t2.print_formatted(num_rows=25)




        # for i in range(0, criteria.num_conditions):
        #     idx = self.__get_column_idx(criteria.conditions[i][0])
        #     if idx is None:
        #         print("column %s is not present in table %s" % (criteria.conditions[i][0], self.name))
        #         return False
            
        #     comparator = OPERATORS[criteria.comparators[i]]
        #     val = criteria.conditions[i][1]

        #     if NUMERIC[comparator]:
        #         c_new = comparator(self.rows[:, idx].astype(int), int(val))
        #     else:
        #         c_new = comparator(self.rows[:, idx], val)

        #     if i - 1 < 0:
        #         c = c_new
        #     else:
        #         logic_operator = OPERATORS[criteria.logic_operators[i-1]]
        #         c = logic_operator(c_new, c)
