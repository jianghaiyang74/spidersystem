import pickle
import threading,socket,os
import time

import redis

class RedisLock(object):

    def __init__(self,lock_name,host="localhost",port=6379,db=0):
        self.redis = redis.StrictRedis(host=host,port=port,db=db)
        self.lock_name = lock_name

    def _get_thread_id(self):
        return socket.gethostname() + str(os.getpid()) + threading.current_thread().name

    def acquire_lock(self,thread_id=None,expire=10,block=True):
        '''

        :param thread_id: 表明每个线程的唯一标示值,用来判断锁
        :return:
        '''
        # 如果lock_name存在 ret=0  否则ret=1
        if thread_id is None:
            thread_id = self._get_thread_id()

        while block:
            ret = self.redis.setnx(self.lock_name, pickle.dumps(thread_id))
            if ret == 1:
                self.redis.expire(self.lock_name, expire)  # 过期时间 去除死锁
                print("上锁成功")
                return True
            time.sleep(0.001)

        ret = self.redis.setnx(self.lock_name,pickle.dumps(thread_id))
        if ret == 1:
            self.redis.expire(self.lock_name, expire)  # 过期时间 去除死锁
            print("上锁成功")
            return True
        else:
            print("上锁失败")
            return False


    def release_lock(self,thread_id=None):
        '''解锁'''
        if thread_id is None:
            thread_id = self._get_thread_id()
        ret = self.redis.get(self.lock_name)
        if ret is not None and pickle.loads(ret) == thread_id:  # 确保是解锁线程还是上锁线程
            self.redis.delete(self.lock_name)
            print("解锁成功")
            return True
        else:
            print("解锁失败")
            return False


if __name__ == '__main__':
    redis_lock = RedisLock("redis_lock1")


    if redis_lock.acquire_lock(expire=10):
        print("执行相应的操作")
        # redis_lock.release_lock()
