def load_from_config(app):
    """
    根据config加载
    :param config: object
    :return: None
    """

    # 根据配置load远程secrets和config
    # from ..security import secrets
    # from ..services import manager

    from ..flask_ext import init_cache
    # 初始化session
    # init_session(app)
    #初始化cache
    init_cache(app)

    # if app.config.get('STORAGE_TYPES'):
    #     if 'postgresql' in app.config.get('STORAGE_TYPES'):
    #         from base.db.postgresql import init_postgresql
    #         init_postgresql(app.config)

        # if 'mongodb' in app.config.get('STORAGE_TYPES'):
    from base.db.mongo import init_mongodb
    init_mongodb(app.config)

    
    # # SSO
    # from .. import file
    #
    # # Tracing
    # from .. import tracing

    # service call
    # from ..services.agent import Service
    # ser = Service()
    # ser.init_app(app)

    # grpc client
    # from ..grpc_lib.grpc_client_init import RemoteGRPCServices
    # grpcs = RemoteGRPCServices()
    # grpcs.init_app(app)

    # if app.config.get('CORS_ENABLE'):
    #     from flask_cors import CORS
    #     # debug 模式允许跨域请求，适合本地开发，在 base.config 中配置
    #     CORS(app)
    # if app.config.get('ENV') != 'product':
    #     from flasgger import Swagger
    #
    #     Swagger(app)