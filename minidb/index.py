from BTrees.OOBTree import OOBTree


"""
Index could be on columns with non-unique elements. So, rather than assigning the (r, c) to self.index,
it should append it to a list. 
"""


class Index:

    def __init__(self, table, col_idx, idx_type):
        self.index = None
        self.type = idx_type
        if idx_type == "Hash":
            self.index = {}
        else:
            self.index = OOBTree()

        for i, row in enumerate(table.rows):
            if row[col_idx] not in self.index:
                self.index[row[col_idx]] = []
            self.index[row[col_idx]].append((i, col_idx))

    def get_pos(self, key):
        if key not in self.index:
            return None
        return self.index[key]

    def print(self, f=None):
        for i in self.index.keys():
            print("%-10s -> %s" % (i, self.index[i]), file=f)
