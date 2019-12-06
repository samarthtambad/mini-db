from minidb.table import Table
import operator
from minidb.utils import Utils as utils
import numpy as np


class Join:
	def __init__(self, t1, t2, criteria):
		self.t1_name=t1.name
		self.t2_name=t2.name
		self.t1=t1.copy("t1_copy")
		self.t2=t2.copy("t2_copy")
		self.criteria=criteria

		self.tables = {t1.name:self.t1, t2.name:self.t2}
		self.table_rows = {t1.name:[], t2.name:[]}

		self.out_rows=[]

	def do_join(self):
		# decide which join to use
		# use merge join if there is one condition, the condition is equality, and there are no duplicates

		if (len(self.criteria.eq_conditions)==1 and len(self.criteria.conditions)==1): #add arithops and (self.criteria.arithops[0][0] is None)
			self.out_rows=self.indexjoin_single(self.criteria.conditions[0]);

		elif (len(self.criteria.eq_conditions)==0):
			self.out_rows=self.nestedloopjoin()
		
		else:
			self.out_rows=self.indexjoin_multiple(self.criteria.conditions[0])

		return self.out_rows


	def nestedloopjoin(self):
		new_rows = []

		for row1 in self.t1.rows:
			for row2 in self.t2.rows:
				if (self.check_conditions(row1, row2)):
					new_row = np.concatenate([row1,row2])
					new_rows.append(new_row)
				else:
					continue
		return new_rows

	def check_conditions(self, row1, row2):
		if (self.criteria.arithops[0][0] is None and self.criteria.arithops[0][1] is None):
			for i in range(0,self.criteria.num_conditions):
					table_a_name, col_a, table_b_name, col_b = self.criteria.conditions[i]

					idx_a=self.tables[table_a_name].col_names[col_a]
					idx_b=self.tables[table_b_name].col_names[col_b]

					if (table_a_name == self.t1_name):
						val_a=row1[idx_a]
						val_b=row2[idx_b]
					else:
						val_a=row2[idx_a]
						val_b=row1[idx_b]
					if (not self.check_condition(i,val_a,val_b)):
						return False
					else:
						continue
			return True
		else:
			# print("handle arithop TODO")
			return



	def check_condition(self, i, val1, val2):
		comparator = utils.OPERATORS[self.criteria.comparators[i]]
		if utils.NUMERIC[comparator]:
			if (not comparator(float(val1),float(val2))):
				return False
		else:
			if (not comparator(val1,val2)):
					return False
		return True




	def indexjoin_single(self, condition):
		if (self.criteria.arithops[0][0] is None and self.criteria.arithops[0][1] is None):
			table1_name, col1, table2_name, col2 = condition
			t1 = self.tables[table1_name]
			idx1 = t1.col_names[col1]   # get index of column in table

			t2 = self.tables[table2_name]
			if col2 not in t2.indexes:
				t2.hash_index(col2)
			t2_idx = t2.indexes[col2]

			new_rows = []
			for i, row1 in enumerate(t1.rows):
				val1 = row1[idx1]
				try:
					val1=float(val1)
				except:
					val1=val1
				pos = t2_idx.get_pos(val1)
				if pos is None:
					continue
				else:
					for p in pos:
						new_row = np.concatenate([row1,t2.rows[p[0]]])
						new_rows.append(new_row)
			return new_rows
		else:
			return self.indexjoin_single_arithop(condition)

	def indexjoin_single_arithop(self, condition):
		del_index=False
		table1_name, col1, table2_name, col2 = condition
		t1 = self.tables[table1_name]
		idx1 = t1.col_names[col1]

		t2 = self.tables[table2_name]
		if (self.criteria.arithops[0][1] is not None):	
			# create new index on transformed keys
			del_index = True #delete transformed index after we are done using it
			t2.apply_hash_transformation(col2, self.criteria.constants[0][1], self.criteria.arithops[0][1])
			t2_idx = t2.indexes[col2+"_tr"]
		elif col2 not in t2.indexes:
			t2.hash_index(col2)
			t2_idx = t2.indexes[col2]
		else:
			t2_idx = t2.indexes[col2]
		
		new_rows = []
		for i, row1 in enumerate(t1.rows):
			val1 = row1[idx1]

			if (self.criteria.arithops[0][0] is not None):
				arithop = self.criteria.arithops[0][0]
				val1 = utils.OPERATORS[arithop](float(val1), float(self.criteria.constants[0][0]))
			else:
				try:
					val1=float(val1)
				except:
					val1=val1

			# print(val1)
			pos = t2_idx.get_pos(val1)

			if pos is None:																
				continue
			else:
				for p in pos:
					new_row = np.concatenate([row1,t2.rows[p[0]]])
					new_rows.append(new_row)
		if (del_index):
			del t2.indexes[col2+"_tr"]
		return new_rows

	def indexjoin_multiple(self, condition):
		table1_name, col1, table2_name, col2 = condition
		self.table_rows[table1_name]=[]
		self.table_rows[table2_name]=[]
		t1 = self.tables[table1_name]
		idx1 = t1.col_names[col1]   # get index of column in table

		t2 = self.tables[table2_name]
		if col2 not in t2.indexes:
			t2.hash_index(col2)
		t2_idx = t2.indexes[col2]

		new_rows = []

		for i, row1 in enumerate(t1.rows):
			val1 = row1[idx1]
			try:
				val1=float(val1)
			except:
				val1=val1
			pos = t2_idx.get_pos(val1)

			if pos is None:
				continue
			else:
				for p in pos:
					if (self.check_remaining_conditions(row1, p)):
						new_row = np.concatenate([row1,t2.rows[p[0]]])
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
			if utils.NUMERIC[comparator]:
				if (not comparator(float(val1),float(val2))):
					return False
			else:
				if (not comparator(val1,val2)):
					return False
			i+=1
		return True



