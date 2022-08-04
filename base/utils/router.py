from flask.blueprints import Blueprint
import logging
logger = logging.getLogger(__name__)


def bp_register(url_sub: dict, bp: Blueprint, url_str="",):
    for key, value in url_sub.items():
        new_str = url_str + key
        if isinstance(value, dict):
            bp_register(value, bp, new_str)
        else:
            bp.add_url_rule(new_str, view_func=value[0], methods=value[1])
            logger.debug("add url {} for class {}".format(bp.url_prefix+new_str, value[0].__name__))

    return bp
