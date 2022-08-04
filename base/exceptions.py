
class MaxTimeExceed(Exception):
    def __init__(self, max_times, *args, **kwargs):
        self.max_times = max_times
        super(*args, **kwargs)


class UnAuthorizedException(Exception):
    pass


class ValidateError(Exception):
    def __init__(self, msg):
        self.msg = msg
