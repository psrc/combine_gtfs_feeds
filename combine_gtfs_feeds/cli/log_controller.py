import logging
from functools import wraps
from time import time
import datetime
import os, sys, errno
import yaml
import shutil
from shutil import copy2 as shcopy


def setup_custom_logger(name, output_dir):
    if os.path.exists(os.path.join(output_dir, 'run_log.txt')):
        os.remove(os.path.join(output_dir, 'run_log.txt'))

    logging.basicConfig(filename= os.path.join(output_dir, 'run_log.txt'),format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    
    handler = logging.StreamHandler()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

def timed(f):
  @wraps(f)
  def wrapper(*args, **kwds):
    main_logger = logging.getLogger('main_logger')

    start = datetime.datetime.now()
    main_logger.info(" %s starting" % (f.__name__))

    result = f(*args, **kwds)

    elapsed = datetime.datetime.now() - start
    main_logger.info("%s took %s" % (f.__name__, str(elapsed)))
    return result
  return wrapper
