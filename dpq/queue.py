# -*- coding: utf-8 -*-

import times

from .connections import resolve_connection
from .exceptions import NoSuchJobError, UnpickleError
from .job import Job


def compact(lst):
    return [item for item in lst if item is not None]


class Queue(object):
    namespace_prefix = "dpq:queue:"

    @classmethod
    def all(cls, connection=None):
        """Return an iterable of all Queues"""
        prefix = cls.namespace_prefix
        if connection is None:
            connection = resolve_connection()

        def to_queue(queue_key):
            return cls.from_queue_key(queue_key, connecion=connection)
        return map(to_queue, connection.keys('%s*' % prefix))

    @classmethod
    def from_queue_key(cls, queue_key, connection=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid DPQ queue key: %s' % queue_key)
        name = queue_key[len(prefix):]
        return Queue(name, connection=connection)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 default_job_timeout=180):
        if connection is None:
            connection = resolve_connection()

        self.connection = connection
        prefix = self.namespace_prefix
        self.name = name
        self._key = '%s%s' % (prefix, name)
        self._default_timeout = default_timeout
        self.default_job_timeout = default_job_timeout

    @property
    def key(self):
        """Return the redis key for this Queue"""
        return self._key

    def empty(self):
        """Remove all message on the Queue"""
        self.connection.delete(self.key)

    def is_empty(self):
        """Return whether the current queue is empty"""
        return self.count == 0

    @property
    def job_ids(self):
        """Return all job ids in the Queue"""
        return self.connection.lrange(self.key, 0, -1)

    @property
    def jobs(self):
        """Return all jobs in the Queue"""
        def safe_fetch(job_id):
            try:
                job = Job.fetch(job_id, self.connection)
            except NoSuchJobError:
                return None
            except UnpickleError:
                return None
            return job
        return compact([safe_fetch(job_id) for job_id in self.job_ids])

    @property
    def count(self):
        """Return a count of all message in the queue"""
        return self.connection.llen(self.key)

    def push_job_id(self, job_id):
        self.connection.rpush(self.key, job_id)

    def enqueue(self, func, *args, **kwargs):
        if func.__module__ == '__main__':
            raise ValueError("Functions from __main__ module cannot be "
                             "processed by workers.")
        timeout = kwargs.pop('timeout', None)
        job = Job.create(func, *args, connection=self.connection, **kwargs)
        return self.enqueue_job(job, timeout=timeout)

    def enqueue_job(self, job, timeout=None, set_meta_data=True):
        if set_meta_data:
            job.origin = self.name
            job.enqueue_at = times.now()

        if timeout is None:
            timeout = self.default_job_timeout
        job.timeout = timeout

        job.save()
        self.push_job_id(job.id)
        return job

    def pop_job_id(self):
        return self.connection.lpop(self.key)
