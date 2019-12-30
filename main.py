import threading
import asyncio
import tornado.ioloop

from spidersystem.request import Request
from spidersystem.requests_manager import RequestManager
from spidersystem.requests_manager.utils.redis_tools import get_redis_queue_cls
from spidersystem.downloader import RequestsDownloader,TornadoDownloader,AsyncTornadoDownloader,ChromeHeadlessDownloader


FIFO_QUEUE = get_redis_queue_cls("fifo")

class Master(object):

    def __init__(self,spiders,request_manager_config,project_name):
        self.filter_queue = FIFO_QUEUE("filter_queue",host="localhost") # 请求过滤队列
        self.request_manager = RequestManager(**request_manager_config)  # 请求管理对象: 请求过滤器,请求队列
        self.spiders = spiders
        self.project_name = project_name

    def run_start_requests(self):
        for spider in self.spiders.values():
            for request in spider().start_requests():
                self.filter_queue.put(request)

    def run_filter_queue(self):
        while True:
            request = self.filter_queue.get()
            self.request_manager.add_request(request, self.project_name)

    def run(self):
        # 改成多线程执行
        #self.run_start_requests()
        #self.run_filter_queue()
        threading.Thread(target=self.run_start_requests).start()
        threading.Thread(target=self.run_filter_queue).start()

import redis
import json

class RequestWatcher(object):
    '''处理失败请求和丢失请求'''

    def __init__(self):
        self.redis_client = redis.StrictRedis()

    def json_serializer(self, request, error=None):
        request_data = {
            "url": request.url,
            "method": request.method,
            "query": request.query,
            "body": request.body,
            "name": request.name,
            "headers": request.headers,
            "id": request.id,
            "error": error
        }
        return json.dumps(request_data)  # 转换为规则的json字符串

    def mark_processing_requests(self, request):
        self.redis_client.hset("processing_requests", request.id, self.json_serializer(request))

    def unmark_processing_requests(self,request):
        self.redis_client.hdel("processing_requests", request.id)

    def mark_fail_requests(self, request, error):
        self.redis_client.hset("fail_requests", request.id, self.json_serializer(request,error))


class Slave(object):

    def __init__(self, spiders,request_manager_config,project_name):
        self.filter_queue = FIFO_QUEUE("filter_queue", host="localhost")  # 请求过滤队列
        self.request_manager = RequestManager(**request_manager_config)  # 请求管理对象: 请求过滤器,请求队列
        self.downloader = ChromeHeadlessDownloader()  # 下载器更换
        self.spiders = spiders
        self.project_name = project_name

        self.request_watcher = RequestWatcher()


    async def handle_request(self):
        # 获取io_loop 实现 某个事件阻塞无法实现协程的情况
        io_loop = tornado.ioloop.IOLoop.current()
        # 1.获取一个请求
        future = io_loop.run_in_executor(None,self.request_manager.get_request,self.project_name)
        #request = self.request_manager.get_request(self.project_name)
        request = await future

        self.request_watcher.mark_processing_requests(request)

        try:
            # 2.发起请求获得响应
            response = await self.downloader.fetch(request)

            # 3.获取爬虫对象
            spider = self.spiders[request.name]()

            # 4.处理response
            for result in spider.parse(response):
                # 如果是request 返回给Master
                if result is None:
                    raise Exception("Not allow is None")
                elif isinstance(result, Request):
                    # 发生网络io事件 变成协程函数
                    await io_loop.run_in_executor(None, self.filter_queue.put, result)
                    #self.filter_queue.put(result)
                else:
                    # 意味着是一个数据
                    new_result = spider.data_clean(result)
                    spider.data_save(new_result)
        except Exception as e:
            self.request_watcher.mark_fail_requests(request,str(e))
            raise
        finally:
            self.request_watcher.unmark_processing_requests(request)

    async def run(self):
        # 循环获取请求
        while True:
            # 控制并发量
            await asyncio.wait([
                self.handle_request(),
                self.handle_request()
            ])




