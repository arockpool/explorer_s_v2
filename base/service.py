import os, ujson, uuid, logging
from base.flask_ext import cache
from sqlalchemy import text


class AbcBaseService:
    pass


class BaseService(AbcBaseService):
    @classmethod
    def flush_add(cls, db, obj):
        try:
            db.add(obj)
            db.flush()
            return obj
        except:
            raise

    @classmethod
    def flush_all(cls, db, objs):
        try:
            db.add_all(objs)
            db.flush()
            return objs
        except:
            raise

    @classmethod
    def get_path_of_static_file(cls, path):
        return os.path.join('./user_center/static', path)

    @classmethod
    def set_quick_details(cls, id_list, expire=3600 * 24):
        """
        设置快速查看详情（上一个,下一个）
        :param id_list:
        :param expire:
        :return:
        """
        if id_list:
            key = uuid.uuid4().hex
            cache.set(key, ujson.dumps(id_list), expire)
            return key

    @classmethod
    def get_quick_details(cls, key, obj_id):
        """
        获取快速查看详情（上一个,下一个）
        :param key:
        :param obj_id:
        :return:
        """
        result = dict(next_id=None, previous_id=None)
        if not key:
            return result
        try:
            id_list = []
            id_list_str = cache.get(key)
            if id_list_str:
                id_list = ujson.loads(id_list_str)
            id_len = len(id_list)
            id_index = id_list.index(str(obj_id))
            result['next_id'] = id_list[id_index + 1] if id_index < id_len - 1 else None
            result['previous_id'] = id_list[id_index - 1] if id_index > 0 else None
        except Exception as e:
            logging.exception(e)
        return result

    @classmethod
    def exist_model_column(cls, enterprise_id, model, column, value, model_id=None):
        """
        检查模型的字段值是否存在
        :param enterprise_id:
        :param model:
        :param column:
        :param value:
        :param model_id:
        :return:
        """
        if not value:
            return False
        q = cls.db.query(model).filter(getattr(model, 'enterprise_id') == enterprise_id,
                                       getattr(model, column) == value,
                                       getattr(model, 'id') != model_id)
        return cls.db.query(q.exists()).scalar()
