import requests
from flask import request
from flask import current_app
from base.utils import debug


class Service:
    @classmethod
    def call(cls, name, endpoint, method='POST', body=None, params={}, data={}, headers={}, timeout=10):
        """
        服务间同步调用请求
        :param name: 服务配置名称
        :param endpoint: 蓝图blueprint内注册的router，如 /data/health、/data/get_orders
        :param method: post、get、put、delete，不区分大小写
        :param body: 字典data
        :param params:
        :param data:
        :param headers: 可选参数
        :param timeout:
        :return:
        """
        try:
            internal_host = current_app.config.get(name)
            url = internal_host + endpoint

            # tracing = zipkin.create_http_headers_for_new_span()
            # headers.update(tracing)
            # api_private.setup_request(headers)
            response = requests.request(method,
                                        url,
                                        params=params,
                                        json=body,
                                        data=data,
                                        cookies=(request and request.cookies) or {},
                                        timeout=timeout if isinstance(timeout, (tuple, list)) else (timeout, None),
                                        headers=headers)

            if response.status_code != 200:
                raise Exception(f'Service request error:{response.status_code}, {response.json()}')
            data = response
        except Exception as e:
            # app.logger.error(e.args)
            # if request:
            #     track_error()
            # else:
            debug.get_debug_detail(e)
        return data


if __name__ == '__main__':
    # from base.services.manager import app
    # ser = Service()
    # ser.init_app(app)
    # ser.get_services_address()

    # data = app.services.call('emi', '/emi/health', 'get')
    pass
