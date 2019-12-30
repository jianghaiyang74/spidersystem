import pickle

from .base import BaseRedisQueue
from .redis_lock import RedisLock


class PriorityRedisQueue(BaseRedisQueue):
    '''利用redis的有序集合来实现数据存取'''
    def qsize(self):
        self.last_qsize = self.redis.zcard(self.name)
        return self.last_qsize

    def put_nowait(self, obj):
        '''

        :param obj:(score,value)
        :return:
        '''
        if self.lazy_limit and self.last_qsize < self.maxsize:
            pass
        elif self.full():
            raise self.Full
        self.last_qsize = self.redis.zadd(self.name,{pickle.dumps(obj[1]):obj[0]})
        return True

    def get_nowait(self):
        '''
        -1,-1: 默认取权重最大的
        0,0: 默认取权重最小的
        :return:
        '''
        if self.use_lock is True:
            if self.lock is None:
                self.lock = RedisLock(**self.redis_lock_config)

            if self.lock.acquire_lock():
                ret = self.redis.zrange(self.name, -1, -1)
                if not ret:
                    raise self.Empty
                self.redis.zrem(self.name,ret[0])
                self.lock.release_lock()

                return pickle.loads(ret[0])

        else:
            ret = self.redis.zrange(self.name, -1, -1)
            if not ret:
                raise self.Empty
            self.redis.zrem(self.name, ret[0])

            return pickle.loads(ret[0])