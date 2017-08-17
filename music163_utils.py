# -*-coding:utf8-*-
import time
import traceback
from datetime import datetime as dt
from functools import wraps
from music163_config import *
import MySQLdb as mdb


class DBAccesor(object):
    def __init__(self, db_dict):
        self.db_setting = db_dict

    def connect_database(self):
        """
        We can't fail in connect database, which will make the subprocess zoombie
        """
        attempt = 1
        for _ in range(16):
            seconds = 3*attempt
            try:
                # WEBCRAWLER_DB_CONN = mdb.connect(**OUTER_MYSQL)
                conn = mdb.connect(**self.db_setting)
                return conn
            except mdb.OperationalError as e:
                print("{now}Sleep {time} seconds cuz we can't connect MySQL...".format(
                    now= dt.now().strftime("%Y-%m-%d %H:%M:%S"), time=seconds
                ))
            except Exception as e:
                traceback.print_exc()
                print("{now}Sleep {time} cuz unknown connecting database error.".format(
                    now=dt.now().strftime("%Y-%m-%d %H:%M:%S"), time=seconds
                ))
            attempt += 1
            time.sleep(seconds)


def get_now(tab=True):
    if tab:
        return dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\t"
    else:
        return dt.now().strftime("%Y-%m-%d %H:%M:%S")


def timestamp2date(time_str):
    if not time_str:
        return ''
    return dt.fromtimestamp(int(time_str)).strftime('%Y-%m-%d')


def timestamp2datetime(time_str):
    if not time_str:
        return ''
    return dt.fromtimestamp(int(time_str)).strftime('%Y-%m-%d %H:%M:%S')


def calc_edit_distance(x, y):
    if isinstance(x, str):
        x = x.decode('utf8')
    if isinstance(y, str):
        y = y.decode('utf8')
    matrix = []
    for i in range(len(x)+1):
        matrix.append([0] * (len(y)+1)) # initialize the matrix with zeros
    for i in range((len(x) +1 )):
        matrix[i][0] = i # Fill in the first column with acscending integers
    for j in range((len(y) + 1)):
        matrix[0][j] = j # Fill in the first row with ascending integers
    for i in range(1, len(x) + 1 ):
        for j in range(1, len(y) + 1):
            # Fill in other elements
            delta = 1 if x[i-1] != y[j-1] else 0
            distDiag = matrix[i-1][j-1] + delta
            distVer = matrix[i-1][j] + 1
            distHor = matrix[i][j-1] + 1
            matrix[i][j] = min(distDiag, distHor, distVer)
    return matrix[-1][-1]


def gen_abuyun_proxy():
    # authorization
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host" : ABUYUN_HOST,
        "port" : ABUYUN_PORT,
        "user" : ABUYUN_USER,
        "pass" : ABUYUN_PASSWD,
    }
    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }
    return proxies


def retry(ExceptionToCheck=(Exception, ), tries=4, delay=3, backoff=2, logger=None):
    """ Retry calling the decorated function using an exponential backoff.
    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
    :param ExceptionToCheck(Exception or tuple): the exception to check. may be a tuple of
        exceptions to check
    :param tries(int): number of times to try (not retry) before giving up
    :param delay(int): initial delay between retries in seconds
    :param backoff: backoff multiplier, sleep 3*2**n seconds
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    traceback.print_exc()
                    mtries -= 1
                    msg = "%s\nRetrying in %d seconds and leave %d times..." % (str(e), mdelay, mtries)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    # handle_proxy_error(mdelay)
                    # handle_sleep(mdelay)
                    time.sleep(mdelay)
                    mdelay *= backoff
            return f(*args, **kwargs) # still raise Exception
        return f_retry  # true decorator
    return deco_retry
