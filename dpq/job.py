# -*- coding: utf-8 -*-

import times
import importlib
from uuid import uuid4
from cPickle import loads, dumps, UnpicklingError

from .connections import resolve_connection
from .exceptions import NoSuchJobError, UnpickleError


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(pickled_string)
    except (StandardError, UnpicklingError):
        raise UnpickleError('Could not unpickle.', pickled_string)
    return obj


def cancel_job(job_id, connection=None):
    Job(job_id, connection=connection).cancel()


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
    def exists(cls, job_id):
        conn = resolve_connection()
        return conn.exists(cls.key_for(job_id))

    @classmethod
    def fetch(cls, id, connection=None):
        job = Job(id, connection=connection)
        job.refresh()
        return job

    def __init__(self, id=None, connection=None):
        if connection is None:
            connection = resolve_connection()
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

    @classmethod
    def key_for(cls, job_id):
        return 'dpq:job:%s' % job_id

    @property
    def key(self):
        return self.key_for(self.id)

    @property
    def job_tuple(self):
        return (self.func_name, self.args, self.kwargs)

    @property
    def return_value(self):
        if self._result is None:
            rv = self.connection.hget(self.key, 'result')
            if rv is not None:
                self._result = loads(rv)
        return self._result

    result = return_value

    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        properties = [
            'data', 'created_at', 'origin', 'description', 'enqueued_at',
            'ended_at', 'result', 'exc_info', 'timeout']
        data, created_at, origin, description, \
            enqueued_at, ended_at, result, \
            exc_info, timeout = self.connection.hmget(key, properties)
        if data is None:
            raise NoSuchJobError('No such job: %s' % (key,))

        def to_date(date_str):
            if date_str is None:
                return None
            else:
                return times.to_universal(date_str)

        self._func_name, self._args, self._kwargs = unpickle(data)
        self.created_at = to_date(created_at)
        self.origin = origin
        self.description = description
        self.enqueued_at = to_date(enqueued_at)
        self.ended_at = to_date(ended_at)
        self._result = result
        self.exc_info = exc_info
        if timeout is None:
            self.timeout = None
        else:
            self.timeout = int(timeout)

    def save(self):
        key = self.key

        obj = {}
        obj['create_at'] = times.format(self.create_at, 'UTC')

        if self.func_name is not None:
            obj['data'] = dumps(self.job_tuple)
        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = times.format(self.enqueued_at, 'UTC')
        if self.ended_at is not None:
            obj['ended_at'] = times.format(self.ended_at, 'UTC')
        if self._result is not None:
            obj['result'] = self._result
        if self.exc_info is not None:
            obj['exc_info'] = self.exc_info
        if self.timeout is not None:
            obj['timeout'] = self.timeout

        self.connection.hmset(key, obj)

    def cancel(self):
        self.delete()

    def delete(self):
        self.connection.delete(self.key)

    def perform(self):
        """Invoke the job function with arguments"""
        self._result = self.func(*self.args, **self.kwargs)
        return self._result

    def get_call_string(self):
        if self.func_name is None:
            return None

        arg_list = [repr(arg) for arg in self.args]
        arg_list += ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)
        return '%s(%s)' % (self.func_name, args)

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)
