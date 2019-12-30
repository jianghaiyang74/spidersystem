import logging

logging.basicConfig(level=logging.INFO)

class RequestManager(object):
    '''请求管理器'''

    def __init__(
            self,
            queue_type="fifo",
            filter_type="redis",
            queue_kwargs={},
            filter_kwargs={}
    ):
        self._queues = {}
        self._filters = {}

        self._filters_kwargs = filter_kwargs  #实例化过滤器对象时
        self._queue_kwargs = queue_kwargs #实例化队列对象时的关键词参数

        self._set_filter_cls(filter_type)  # 设置self._filter_cls 属性
        self._set_queue_cls(queue_type)  # 设置self._queue_cls属性


    def _set_filter_cls(self,filter_type):
        from spidersystem.requests_manager.utils.filter_class import get_filter_class
        self._filters_cls = get_filter_class(filter_type)


    def _set_queue_cls(self,queue_type):
        from spidersystem.requests_manager.utils.redis_tools import get_redis_queue_cls
        self._queue_cls = get_redis_queue_cls(queue_type)


    def _get_request_filter(self,filter_name):
    # 判断指定的请求过滤器是否已经存在
    # 如已经存在
        # 直接返回
    # 如不存在:
        # 那么先创建指定的请求过滤器,存储起来后,在返回
        if filter_name in self._filters:
            return self._filters[filter_name]
        else:
            from .requests_filter import RequestFilter
            self._filters_kwargs["redis_key"] = filter_name
            self._filters_kwargs["mysql_table_name"] = filter_name
            filter_obj = self._filters_cls(**self._filters_kwargs)
            request_filter = RequestFilter(filter_obj)

            self._filters[filter_name] = request_filter
            return request_filter


    def _get_request_queue(self,queue_name):
        if queue_name in self._queues:
            return self._queues[queue_name]
        else:
            self._queue_kwargs["name"] = "request-queue-" + queue_name
            queue_obj = self._queue_cls(**self._queue_kwargs)
            self._queues[queue_name] = queue_obj
            return queue_obj

    def add_request(self,request_obj,queue_name,filter_name=None):
        '''
        对请求进行去重,并将非重复的请求对象添加到指定请求队列中
        :param request_obj: request object 或者 (优先级权重值,request)
        :param queue_name: 在请求管理对象中存储的请求队列的名称
        :param filter_name: 在请求管理对象中存储的过滤器的名称
        :return:
        '''
        # 获取请求去重对象,并进行请求去重判断
        # 如果重复:
        # 那么记录下重复的请求,如用日志
        # 如果不重复:
        # 根据队列名称获取请求队列
        # 向队列中添加请求对象
        # 添加完成后,在请求去重容器中添记该请求

        # 如果使用的是优先级队列,那么request_obj 应当是一个二元元组
        if isinstance(request_obj, tuple):
            priority,request = request_obj

        else:
            priority = None
            request = request_obj

        if filter_name is None:
            # 如果过滤器和队列使用redis的话,那么filter_name和queue_name不能相同
            filter_name = "filter-" + queue_name

        request_filter = self._get_request_filter(filter_name)
        request_queue = self._get_request_queue(queue_name)

        if request_filter.is_exists(request):
            logging.info("发现重复请求: <%s>"% request.url)
        else:
            request.id = request_filter.mark_request(request)

            if priority is None:
                request_queue.put(request)
            else:
                request_queue.put((priority,request))

            logging.info("请求添加成功: <%s>"%request.url)

    def get_request(self,queue_name,block=True):
        '''
        从指定队列中获取请求对象
        :param queue_name:
        :param block:
        :return:
        '''
        # 根据请求名称拿到请求队列
        # 从请求队列获取请求对象

        request_queue = self._get_request_queue(queue_name)
        request = request_queue.get(block=block)
        return request