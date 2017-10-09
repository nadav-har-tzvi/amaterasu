import os
import shutil
import stat
import errno
from tests.compat import *
from amaterasu.compat import *


def handleRemoveReadonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
        func(path)
    else:
        raise exc[0]


def before_scenario(context, scenario):
    context.stats_before = {}
    context.stats_after = {}
    try:
        shutil.rmtree(os.path.abspath('tmp'), onerror=handleRemoveReadonly)
    except (FileNotFoundError, WindowsError):
        pass
    os.mkdir(os.path.abspath('tmp'))


def after_scenario(context, scenario):
    shutil.rmtree(os.path.abspath('tmp'), onerror=handleRemoveReadonly)
