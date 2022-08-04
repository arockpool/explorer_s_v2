import redis
# from flask_session import Session
import logging
from flask_caching import Cache
logger = logging.getLogger(__name__)
# session = Session()
cache = Cache(with_jinja2_ext=False)
# session_redis = None

# def init_session(app):
#     global session_redis
#     session_redis = redis.StrictRedis(
#         host=app.config.get('SESSION_REDIS_HOST', 'localhost'),
#         port=app.config.get('SESSION_REDIS_PORT', 6379),
#         db=app.config.get('SESSION_REDIS_DB', 0),
#         password=app.config.get('SESSION_REDIS_PASSWORD', '')
#     )
#     app.config.update({
#         'SESSION_REDIS': session_redis
#     })
#     session.init_app(app)


def init_cache(app):
    global cache
    try:
        cache.init_app(app)
    except Exception as e:
        logger.debug('Init cache: {}'.format(e.args[0]))
        cache = None
