# 基于python中的set集合 数据结构进行去重 为判断依据的存储

from . import BaseFilter

class MemoryFilter(BaseFilter):

    def _get_storage(self):
        return set()

    def _save(self,hash_value):
        '''利用set进行存储'''
        return self.storage.add(hash_value)


    def _is_exists(self,hash_value):
        if hash_value in self.storage:
            return True
        return False