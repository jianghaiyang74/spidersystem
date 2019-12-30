# 实现请求去重的逻辑
import urllib.parse

class RequestFilter(object):

    def __init__(self,filter_obj):
        self.filter_obj = filter_obj

    def is_exists(self,request_obj):
        '''判断请求是否已经处理过
        return True or False
        '''
        data = self._get_request_filter_data(request_obj)

        return self.filter_obj.is_exists(data)

    def mark_request(self,request_obj):
        '''
        标记已经处理过的请求对象
        :param request_obj:
        :return: 标记
        '''
        data = self._get_request_filter_data(request_obj)
        return self.filter_obj.save(data)

    def _get_request_filter_data(self,request_obj):
        '''
        根据一个请求对象,处理他的数据,转换为字符串,然后进行去重处理
        :param request_obj:
        :return: 转换后的字符串
        '''
        # 把协议和域名部分进行大小写统一,其它保留原始大小写格式
        # 对查询参数进行简单的排序
        url = request_obj.url
        _ = urllib.parse.urlparse(url)
        url_without_query = _.scheme + "://" + _.hostname + _.path
        url_query = urllib.parse.parse_qsl(_.query)

        method = request_obj.method.upper()

        query = request_obj.query.items()
        all_query = sorted(set(list(query) + url_query))
        url_with_query = url_without_query + "?" + urllib.parse.urlencode(all_query)
        str_body = str(sorted(request_obj.body.items()))

        data = url_with_query + method + str_body
        return data
