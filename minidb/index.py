from BTrees.OOBTree import OOBTree


class Index:

    def __init__(self, table, col_idx, idx_type, transform_criteria=None):
        self.index = None
        self.type = idx_type
        self.table = table
        self.transform_criteria = transform_criteria
        # table.indexes[col_idx]=idx_type

        if idx_type == "Hash":
            self.create_hash_index(col_idx)
        elif idx_type == "Hash_Transform":
            self.create_transform_hash_index(col_idx)
        else:
            self.index = OOBTree()
            for i, row in enumerate(table.rows):
                self.index[row[col_idx]] = (i, col_idx)

    def create_hash_index(self, col_idx):
        self.index = {}
        if self.table.is_col_numeric(col_idx):
            for i, row in enumerate(self.table.rows):
                k = float(row[col_idx])
                if k in self.index.keys():
                    self.index[k].append((i, col_idx))
                else:
                    self.index[k] = [(i, col_idx)]
    
        else: #indexed column is not numberic ; keys should be strings
            for i, row in enumerate(self.table.rows):
                k = row[col_idx]
                if k in self.index.keys():
                    self.index[k].append((i, col_idx))
                else:
                    self.index[k] = [(i, col_idx)]

    def create_transform_hash_index(self, col_idx):
        # assuming transformed coclumns are numeric
        self.index = {}
        arithop = self.transform_criteria[0]
        constant = self.transform_criteria[1]
        for i, row in enumerate(self.table.rows):
            key = row[col_idx]
            transformed_key = arithop(float(key), float(constant))
            if transformed_key in self.index.keys():
                self.index[transformed_key].append((i, col_idx))
            else:
                self.index[transformed_key] = [(i, col_idx)]

    def get_pos(self, key):
        if key in self.index.keys():
            return self.index[key]
        else:
            return None

    def print(self, f=None):
        for k, v in self.index.items():
            print("%-10s -> %s" % (k, v), file=f)



