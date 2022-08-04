import logging


def init_logger(app):
    """
    gunicorn 模式下，替换flask log handler
    :param app:
    :return:
    """
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    for handler in app.logger.handlers:
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)s %(filename)s %(funcName)s:%(lineno)s: %(message)s'
        ))
    # app.logger.info("Hello World")
