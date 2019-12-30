
def get_filter_class(cls_name,salts=None):
    '''返回对应的过滤器的类对象'''
    if cls_name == "bloom":
        from .bloomfilter import BloomFilter
        return BloomFilter
    elif cls_name == "memory":
        from .information_summary_filter import MemoryFilter
        return MemoryFilter
    elif cls_name == "mysql":
        from .information_summary_filter import MysqlFilter
        return MysqlFilter
    elif cls_name == "redis":
        from .information_summary_filter import RedisFilter
        return RedisFilter