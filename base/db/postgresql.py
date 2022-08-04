# import os
# import enum
# from sqlalchemy import create_engine, Column, DateTime, Integer, Boolean, MetaData
# from sqlalchemy.orm import sessionmaker, scoped_session, Query
# from sqlalchemy.ext.declarative import declarative_base
# import contextlib
# import random
# from functools import wraps
# from datetime import datetime, date
# from werkzeug.local import LocalStack, release_local
# import logging
#
# logger = logging.getLogger(__name__)
# db_manager = None
#
# if "GEVENT_SUPPORT" in os.environ:
#
#     # Do our monkey patching
#
#     from psycogreen.gevent import patch_psycopg
#
#     patch_psycopg()
#
#     using_gevent = True
# else:
#     using_gevent = False
#
# _db_session = LocalStack()
#
#
# def _set_session(session_obj):
#     _db_session.push(session_obj)
#     return session_obj
#
#
# def _get_session():
#     try:
#         return _db_session.top
#     except Exception as e:
#         logger.exception(e)
#         return None
#
#
# # class BaseQuery(Query):
# #     def __iter__(self):
# #         sql = str(self.statement)
# #         str.find('user_id')
# #         for entity in self._entities:
# #             'user_center.models.User'
# #         return super().__iter__()
#
#
# class DBManager(object):
#     def __init__(self, name, master, slaves=[], scopefunc=None):
#         self.db_name = name
#         self.slave_session_map = {}
#         self.master_session = None
#         self.scopefunc = scopefunc
#         self.create_master(master)
#         self.create_slaves(slaves)
#
#     def create_master(self, master):
#         self.master_session = self.create_single_session(master + '/' + self.db_name, autoflush=True,
#                                                          scopefunc=self.scopefunc)
#
#     def create_slaves(self, slaves=[]):
#         for i in range(0, len(slaves)):
#             slave = self.create_single_session(slaves[i] + '/' + self.db_name, scopefunc=self.scopefunc)
#             if slave:
#                 self.slave_session_map['slave_' + str(i)] = slave
#
#     @classmethod
#     def create_single_session(cls, url, autoflush=False, scopefunc=None):
#         engine = create_engine(url, pool_size=15, strategy="threadlocal")
#
#         return scoped_session(
#             sessionmaker(
#                 expire_on_commit=False,
#                 bind=engine,
#                 autoflush=autoflush
#             ),
#             scopefunc=scopefunc
#         )
#
#     def get_session(self, name):
#         if name == 'master' or not self.slave_session_map:
#             return self.master_session
#
#         if name == 'slave':
#             try:
#                 name = random.choice(list(self.slave_session_map.keys()))
#                 return self.slave_session_map[name]
#             except KeyError:
#                 raise KeyError('{} not created, check your DB_SETTINGS'.format(name))
#             except IndexError:
#                 raise IndexError('cannot get names from DB_SETTINGS')
#         return None
#
#     @contextlib.contextmanager
#     def session_ctx(self, bind='master'):
#         DBSession = self.get_session(bind)
#         session = _set_session(DBSession())
#         try:
#             yield session
#             session.commit()
#         except:
#             session.rollback()
#             raise
#         finally:
#             session.expunge_all()
#             session.close()
#             release_local(_db_session)
#
#
# def init_postgresql(config):
#     global db_manager
#     db_manager = DBManager(
#         config.get('POSTGRESQL_NAME'),
#         master=config.get('POSTGRESQL_MASTER'),
#         slaves=config.get('POSTGRESQL_SLAVES'),
#         # scopefunc=_db_session
#     )
#
#
# def db_session(bind='master'):
#     def decorator(f):
#         @wraps(f)
#         def decorated_function(*args, **kwargs):
#             # try:
#             obj = args[0]
#             db = _get_session()
#             if db:
#                 # logger.debug(f'2  use db session id:{id(db)}; thread: {threading.current_thread()}')
#                 obj.db = db
#                 return f(*args, **kwargs)
#             else:
#                 with db_manager.session_ctx(bind=bind) as db:
#                     # logger.debug(f'1 use db session id:{id(db)}; thread: {threading.current_thread()}')
#                     obj.db = db
#                     return f(*args, **kwargs)
#
#         return decorated_function
#
#     return decorator
#
#
# _BaseModel = declarative_base()
#
#
# class BaseModel(_BaseModel):
#     __abstract__ = True
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     create_time = Column(DateTime, default=lambda: datetime.now())
#     write_time = Column(DateTime, default=lambda: datetime.now(), onupdate=lambda: datetime.now())
#     active = Column(Boolean, default=True)
#
#     def to_dict(self, only_fields=[], exclude_fields=[], bool_fields=[]):
#         """
#         Modle转化为dict
#         :param only_fields:  有值时，只返回指定字段的值 exclude_fields 失效
#         :param exclude_fields: 有值时, 返回值排除
#         :param bool_fields: 有值时, 将值转成bool类型
#         :return:
#         """
#
#         def get_val(val):
#             """
#             类型识别
#             :param val:
#             :return:
#             """
#             if isinstance(val, datetime):
#                 val = val.strftime('%Y-%m-%d %H:%M')
#             if isinstance(val, date):
#                 val = val.strftime('%Y-%m-%d')
#             if isinstance(val, enum.Enum):
#                 val = val.value
#             return val
#
#         tmp = dict()
#         # 只获取的值
#         for field in only_fields:
#             value = get_val(getattr(self, field, None))
#             if field in bool_fields:
#                 value = bool(value)
#             tmp[field] = value
#         if tmp:
#             return tmp
#
#         # 正常处理
#         for c in self.__table__.columns:
#             if c.name in exclude_fields:
#                 continue
#             value = get_val(getattr(self, c.name, None))
#             if c.name in bool_fields:
#                 value = bool(value)
#             tmp[c.name] = value
#
#         return tmp
