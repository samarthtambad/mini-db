
from BTrees.OOBTree import OOBTree


# also trying to combine both in one
class Index:

    def __init__(self, table, col_idx, idx_type):
        self.index = None
        if idx_type == "Hash":
            self.index = {}
        else:
            self.index = OOBTree()

        self.__create()

    def get_pos(self):
        pass

    def __create(self):
        pass

    def print(self, f=None):
        print(self.index, file=f)
        # print("key", "|", "value", file=f)
        # for key, idx in self.my_dict.items():
        #     print(str(key), "|", str(self.data[idx]), file=f)


class HashIndex:

    def __init__(self, table, col_idx):
        self.index = {}
        self.__create()

    def get_pos(self):
        pass

    def __create(self):
        pass

    def print(self, f=None):
        print(self.index, file=f)
        # print("key", "|", "value", file=f)
        # for key, idx in self.my_dict.items():
        #     print(str(key), "|", str(self.data[idx]), file=f)


class BtreeIndex:

    def __init__(self, table, col_idx):
        self.index = OOBTree()
        self.__create()

    def get_pos(self):
        pass

    def __create(self):
        pass

    def print(self, f=None):
        print(self.index, file=f)
        # print("key", "|", "value", file=f)
        # for key, idx in self.tree.iteritems():
        #     print(str(key), "|", str(self.data[idx]), file=f)




