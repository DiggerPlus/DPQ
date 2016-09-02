# -*- coding: utf-8 -*-

from .connections import get_current_connection


class Queue(object):
    namespace_prefix = "dpq:queue:"

    @classmethod
    def all(cls, connection=None):
        """Return an iterable of all Queues"""
        prefix = cls.namespace_prefix
        if connection is None:
            connection = get_current_connection()

        def to_queue(queue_key):
            return cls.from_queue_key(queue_key, connecion=connection)
        return map(to_queue, connection.keys('%s*' % prefix))
