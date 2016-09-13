# -*- coding: utf-8 -*-

import times

from .connections import resolve_connection
from .exceptions import NoSuchJobError, UnpickleError, InvalidJobOperationError
from .job import Job


def get_failed_queue(connection=None):
    return FailedQueue(connection=connection)


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
            return cls.from_queue_key(queue_key, connection=connection)
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

    def compat(self):
        """Remove all dead jobs from queue by cycling through it, while
        guarantueeing FIFO semamtics.
        """
        COMPAT_QUEUE = 'dpq:queue:_compat'
        self.connection.rename(self.key, COMPAT_QUEUE)
        while True:
            job_id = self.connection.lpop(COMPAT_QUEUE)
            if job_id is None:
                break
            if Job.exists(job_id):
                self.connection.rpush(self.key, job_id)

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

    @classmethod
    def lpop(cls, queue_keys, blocking):
        conn = resolve_connection()
        if blocking:
            queue_key, job_id = conn.blpop(queue_keys)
            return queue_key, job_id
        else:
            for queue_key in queue_keys:
                blob = conn.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self):
        """Dequeue the front-most job from this queue.

        Return a Job instance, which can be executed or inspected.
        """
        job_id = self.pop_job_id()
        if job_id is None:
            return None
        try:
            job = Job.fetch(job_id, self.connection)
        except NoSuchJobError as e:
            return self.dequeue()
        except UnpickleError as e:
            e.queue = self
            raise e
        return job

    @classmethod
    def dequeue_any(cls, queues, blocking, connection=None):
        """Class method returning the Job instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `blocking` argument,
        either blocks execution of this function until new messages arrive on
        any of the queues, or returns None.
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, blocking)
        if result is None:
            return None
        queue_key, job_id = result
        queue = Queue.from_queue_key(queue_key, connection=connection)
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return cls.dequeue_any(queues, blocking, connection=connection)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):  # noqa
        return 'Queue(%r)' % (self.name,)

    def __str__(self):
        return '<Queue \'%s\'>' % (self.name,)


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__('filed', connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).

        This is different from normal job enqueueing, since certain meta data
        must not be overridden (e.g. `origin` or `enqueued_at`) and other meta
        data must be inserted (`ended_at` and `exc_info`).
        """
        job.ended_at = times.now()
        job.exc_info = exc_info
        return self.enqueue_job(job, set_meta_data=False)

    def requeue(self, job_id):
        """Requeues the job with the given job ID."""
        try:
            job = Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.connection.lrem(self.key, job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.connection.lrem(self.key, job.id) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

        job.exc_info = None
        q = Queue(job.origin, connection=self.connection)
        q.enqueue_job(job)
