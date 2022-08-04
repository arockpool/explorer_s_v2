import sys
import traceback
import datetime
import logging


class Frame(object):

    def __init__(self, tb):
        self.tb = tb
        frame = tb.tb_frame
        self.locals = {}
        self.locals.update(frame.f_locals)

    def print_path(self):
        return traceback.format_tb(self.tb, limit=1)[0]

    def print_local(self):
        return "\n".join(["%s=%s" % (k, self.dump_value(self.locals[k])) for k in self.locals])

    def dump_value(self, v):
        try:
            return str(v)
        except:
            return "value can not serilizable"


def get_debug_detail(e, log_it=True):
    """
    生成日志相关描述信息
    :param e: Exception
    :param log_it:是否写入日志模块
    :return:
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    frames = []
    tb = exc_traceback
    frames.append(tb.tb_frame)
    detail = "system error -Exception:\n%s\n\ndetail info is:\n" % e
    while tb.tb_next:
        tb = tb.tb_next
        fm = Frame(tb)
        detail += fm.print_path()
        detail += "\nlocals variables:\n"
        detail += fm.print_local()
        detail += "\n" + "-" * 100 + "\n"
    if log_it:
        logging.error(detail)
    return detail
if __name__ == '__main__':
    try:
        1/0
    except Exception as e:
        get_debug_detail(e)

