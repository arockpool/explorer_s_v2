import logging
import importlib
import os, sys
from flask import Flask

from base.utils.load import load_from_config
from base.utils.dict import dict_diff
# from base.services.manager import get_consul_client


def log_diff(logger, d1, d2):
    adds, dels, changes, remains = dict_diff(d1, d2)
    for a in adds:
        logger.debug(f"增加配置{a}: {d2.get(a)}")
    for d in dels:
        logger.debug(f"删除配置{d}: {d1.get(d)}")
    for c in changes:
        logger.debug(f"更改配置{c}: {d1.get(c)} ---> {d2.get(c)}")
    # for r in remains:
    #     logger.debug(f"保留配置{r}: {d2.get(r)}")


def get_server_params():
    import argparse
    c = argparse.ArgumentParser()
    c.add_argument("--name", "-n", '-x', help="server name")
    c.add_argument("--bind", "-b", help="host and port")
    c.add_argument("--debug", "--log-level", "-l", help="debug", default="DEBUG")
    args, _ = c.parse_known_args()
    name = args.name or "explorer"
    debug = getattr(args, 'debug') or 'INFO'
    port = (args.bind and args.bind.split(":")[-1]) or ('gunicorn' in sys.argv and 8000)
    assert name, 'Not set Server'
    print(name)
    return name, int(port), debug


def load_and_log_config(app, load_method,):
    pre = app.config.copy()
    load_method()
    log_diff(app.logger, pre, app.config)


module_name, module_port, debug = get_server_params()


def load_flask_config(app, setting=None):
    """
    加载配置文件
    :param app:
    :return:
    """
    # 加载基础配置

    setting = setting or {}
    app.logger.debug('---- load base config')
    app.config.from_object('base.config')

    # 加载模块配置
    try:
        # 加载模块
        importlib.import_module(app.name)
        # 加载模块配置
        config = importlib.import_module('.config', app.name)

        app.logger.debug('---- load module config')
        load_and_log_config(app, lambda: app.config.from_object(config))
    except Exception as e:
        app.logger.exception(e)
        app.logger.warning(f'{app.name} Config load fail')

    # 加载远程配置
    # remote_config_name = setting.get('remote_config_name') or app.name
    # app.logger.debug('---- load remote config')
    # consul = get_consul_client(app)
    # load_and_log_config(app, lambda: consul.load_remote_config(app, remote_config_name))
    # app.logger.debug('---- overwriting config')
    load_and_log_config(app, lambda: app.config.from_mapping(setting))


def create_app(setting=None):
    """
    创建app并进行app基础配置
    :return:
    """
    # Init App
    app = Flask(module_name)
    app.app_context().push()

    # 设置logger
    _debug_level = debug.upper() or logging.DEBUG
    logging.root.handlers = []
    logging.basicConfig(level=_debug_level, format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    app.logger.addHandler(logging.StreamHandler(sys.stdout))
    app.logger.setLevel(_debug_level)

    # 加载配置
    load_flask_config(app, setting)

    # 根据配置加载模块
    load_from_config(app)

    return app


def init_web(app):
    """
    注册web相关，路由，中间件等
    :param app:
    :return:
    """
    from flask import Blueprint
    from base.utils.router import bp_register
    from base import middleware
    if module_port:
        app.config.update(SERVER_PORT=module_port)

    # 注册路由, 依赖于每个模块中的router.url_map， router.url_prefix
    router = importlib.import_module(".router", app.name)
    bp = Blueprint(app.name, app.name, url_prefix=f"/{app.name}_v2")
    bp_register(router.url_map, bp)
    app.register_blueprint(bp)

    # 注册路由2,依赖于每个模块中的router.url_map_open_api
    if router.url_map_open_api:
        bp_open_api = Blueprint('open_api', app.name, url_prefix='/open_api/explorer')
        bp_register(router.url_map_open_api, bp_open_api)
        app.register_blueprint(bp_open_api)

    # 初始化中间件
    middleware.init(app)

    # 注册web健康检查
    # consul = get_consul_client(app)
    # consul.register_service(app)
