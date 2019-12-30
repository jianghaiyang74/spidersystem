# 基于信息摘要算法进行数据的去重判断和存储

# 1.基于内存的存储
# 2.基于redis的存储
# 3.基于mysql的存储

import hashlib
import six

class BaseFilter(object):
    '''基于信息摘要算法进行数据的去重判断和存储'''

    def __init__(self,hash_func_name="md5",redis_host="localhost",redis_port=6379,redis_db=0,redis_key="filter",mysql_url=None,mysql_table_name="filter"):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.mysql_url = mysql_url
        self.mysql_table_name = mysql_table_name

        self.hash_func = getattr(hashlib,hash_func_name)
        self.storage = self._get_storage()


    def _safe_data(self,data):
        '''

        :param data: 给定的原始数据
        :return: 二进制的字符串数据
        '''
        if six.PY3:
            if isinstance(data,bytes):
                return data
            elif isinstance(data,str):
                return data.encode()
            else:
                raise Exception("Please provide a string")
        else:
            if isinstance(data,str):
                return data
            else:
                raise Exception("Please provide a string")

    def _get_hash_value(self, data):
        '''
        根据跟定的数据,返回对应的信息摘要hash值
        :param data: 给定的原始数据(二进制类型的字符串数据)
        :return: hash值
        '''
        hash_obj = self.hash_func()
        hash_obj.update(self._safe_data(data))
        hash_value = hash_obj.hexdigest()
        return hash_value

    def save(self,data):
        '''
        根据data计算出相应的指纹进行存储
        :param data: 给定的原始数据
        :return: 存储的结果
        '''
        hash_value = self._get_hash_value(data)
        self._save(hash_value)
        return hash_value

    def _save(self,hash_value):
        '''
        存储对应的hash值
        交给对应的子类去继承
        :param hash_value: 通过信息摘要算法求出hash值
        :return: 存储的结果
        '''
        pass

    def is_exists(self,data):
        '''
        判断给定的数据相应的指纹是否存在
        :param data: 给定的原始数据(二进制类型的字符串数据)
        :return: True or False
        '''
        hash_value = self._get_hash_value(data)
        return self._is_exists(hash_value)

    def _is_exists(self,hash_value):
        '''
        判断给定的hash值是否已经存在(交给对应的子类去继承)
        :param data: 通过信息摘要算法求出hash值
        :return: True or False
        '''
        pass

    def _get_storage(self):
        '''
        返回对应的一个存储对象(交给子类继承)
        :return:
        '''
        pass

from .mysql_filter import MysqlFilter
from .memory_filter import MemoryFilter
from .redis_filter import RedisFilter
