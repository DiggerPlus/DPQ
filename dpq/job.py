# -*- coding: utf-8 -*-

import times
import importlib
from uuid import uuid4

from .connections import get_current_connection


class Job(object):

    @classmethod
    def create(cls, func, *args, **kwargs):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        connection = kwargs.pop('connection', None)
        if connection is None:
            raise RuntimeError('Connection cannot be None')
        job = Job(connection=connection)
        job._func_name = '%s.%s' % (func.__module__, func.__name__)
        job._args = args
        job._kwargs = kwargs
        job.description = job.get_call_string()
        return job

    @property
    def func_name(self):
        return self._func_name

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        module_name, func_name = func_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @classmethod
    def fetch(cls, id, connection=None):
        job = Job(id, connection=connection)
        job.refresh()
        return job

    def __init__(self, id=None, connection=None):
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        self._id = id
        self.create_at = times.now()
        self._func_name = None
        self._args = None
        self._kwargs = None
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None

    def get_id(self):
        if self._id is None:
            self._id = unicode(uuid4())
        return self._id

    def set_id(self, value):
        self._id = value

    id = property(get_id, set_id)

    def get_call_string(self):
        if self.func_name is None:
            return None

        arg_list = [repr(arg) for arg in self.args]
        arg_list += ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)
        return '%s(%s)' % (self.func_name, args)
