# -*- coding: utf-8 -*-

"""
  DPQ
  ~~~~~~~

  Simple job queue for python.
"""

from .connections import (
    get_current_connection,
    use_connection,
    push_connection,
    pop_connection,
    Connection)
from .queue import Queue
from .job import cancel_job
from .worker import Worker

__all__ = ['get_current_connection', 'use_connection', 'push_connection',
           'pop_connection', 'Connection', 'Queue', 'cancel_job', 'Worker']

version_info = (0, 0, 1)
__version__ = ".".join([str(v) for v in version_info])
