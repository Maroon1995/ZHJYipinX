from time import time
from ZHJYipinX.util.LogUtil import MyLog

def get_time(f):

    def inner(*arg,**kwarg):
        s_time = time()
        res = f(*arg,**kwarg)
        e_time = time()
        MyLog().info('耗时：{}秒'.format(e_time - s_time))
        return res
    return inner