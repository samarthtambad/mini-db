from minidb.table import Table
import operator
from minidb.utils import Utils as utils
import numpy as np


class Join:
	def __init__(self, t1, t2, criteria):
		self.t1=t1.copy("t1_copy")
		self.t2=t2.copy("t2_copy")
		self.criteria=criteria

		self.tables = {t1.name:self.t1, t2.name:self.t2}
		self.table_rows = {t1.name:[], t2.name:[]}

		self.out_rows=[]

	def do_join(self):
		# decide which join to use
		# use merge join if there is one condition, the condition is equality, and there are no duplicates

		if (len(self.criteria.eq_conditions)==1 and len(self.criteria.conditions)==1):
			self.out_rows=self.indexjoin_single(self.criteria.conditions[0]);
		elif (len(self.criteria.eq_conditions)==0):
			print("TO DO")
			self.out_rows=[]
		else:
			self.out_rows=self.indexjoin_multiple(self.criteria.conditions[0])

		return self.out_rows

	def indexjoin_single(self, condition):
		table1_name, col1, table2_name, col2 = condition
		t1 = self.tables[table1_name]
		idx1 = t1.col_names[col1]   # get index of column in table

		t2 = self.tables[table2_name]
		if col2 not in t2.index:
			t2.hash_index(col2)
		t2_idx = t2.index[col2]

		new_rows = []

		for i, row1 in enumerate(t1.rows):
			val1 = row1[idx1]
			pos = t2_idx.get_pos(val1)
			if pos is None:
				continue
			else:
				for p in pos:
					new_row = np.concatenate([row1,t2.rows[p[0]]])
					new_rows.append(new_row)
		return new_rows

	def indexjoin_multiple(self, condition):
		print(condition)
		table1_name, col1, table2_name, col2 = condition
		self.table_rows[table1_name]=[]
		self.table_rows[table2_name]=[]
		t1 = self.tables[table1_name]
		idx1 = t1.col_names[col1]   # get index of column in table

		t2 = self.tables[table2_name]
		if col2 not in t2.index:
			t2.hash_index(col2)
		t2_idx = t2.index[col2]

		new_rows = []

		for i, row1 in enumerate(t1.rows):
			val1 = row1[idx1]
			pos = t2_idx.get_pos(val1)

			if pos is None:
				continue
			else:
				for p in pos:
					if (self.check_remaining_conditions(row1, p)):
						new_row = np.concatenate([row1,t2.rows[p[0]]])
						# print(new_row)
						new_rows.append(new_row)
		return new_rows

	def check_remaining_conditions(self, row1, p):
		i=1
		while (i<self.criteria.num_conditions):
			table1, col1, table2, col2  = self.criteria.conditions[i]
			idx1 = self.t1.col_names[col1]
			val1=row1[idx1]

			idx2=self.t2.col_names[col2]
			val2=self.t2.rows[p[0]][idx2]

			comparator = utils.OPERATORS[self.criteria.comparators[i]]
			print(comparator)
			print(val1)
			print(val2)
			if utils.NUMERIC[comparator]:
				c = comparator(float(val1),float(val2))
			else:
				c = comparator(val2,val2)
			print(comparator(val1,val2))
			if (not c):
				return False
			i+=1
		return True

	def mergejoin(self):
		self.sort_tables()
		t1_max=self.t1.num_rows
		t2_max=self.t2.num_rows
		t1_col = self.conditions[0][1]
		t2_col = self.conditions[0][3]

		t1_row,t2_row=0,0

		op = utils.OPERATORS[self.comparators[0]]

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
		
	def nestedloop_sorted():
		for t1_row, row1 in enumerate(self.t1.rows):
			for t2_row, row2 in enumerate(self.t2.rows):
				t1_val = float(self.t1.get_value(t1_row, t1_col))
				t2_val = float(self.t2.get_value(t2_row, t2_col))

				if (t1_val > t2_val):
					new_row=self.t1.get_row(t1_row).tolist() + self.t2.get_row(t2_row).tolist()
					self.out_rows.append(new_row)
					
				elif (t1_val == t2_val):
					continue
				elif (t1_val < t2_val):
					continue
		return self.out_rows

	def sort_tables(self):
		# sort tables on columns to join on (using first join condition)
		if (len(self.conditions)>0):
			self.t1.rows=self.t1.sort(None, [self.conditions[0][1]])
			self.t2.rows=self.t2.sort(None, [self.conditions[0][3]])



