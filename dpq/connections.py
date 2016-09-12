# -*- coding: utf-8 -*-

import contextlib
from functools import partial

from redis import StrictRedis, Redis

from .local import LocalStack


_connection_stack = LocalStack()


class NoRedisConnectionException(Exception):
    pass


def fix_return_type(func):
    # deliberately no functools.wraps() call here, since the function being
    # wrapped is a partial, which has no module
    def _inner(*args, **kwargs):
        value = func(*args, **kwargs)
        if value is None:
            value = -1
        return value
    return _inner

PATCHED_METHODS = ['_setex', '_lrem', '_zadd', '_pipeline', '_ttl']


def _hset(self, key, field_name, value, pipeline=None):
    connection = pipeline if pipeline is not None else self
    connection.hset(key, field_name, value)


def patch_connection(connection):
    if all([hasattr(connection, attr) for attr in PATCHED_METHODS]):
        return connection

    connection._hset = partial(_hset, connection)

    if isinstance(connection, Redis):
        connection._setex = partial(StrictRedis.setex, connection)
        connection._lrem = partial(StrictRedis.lrem, connection)
        connection._zadd = partial(StrictRedis.zadd, connection)
        connection._pipeline = partial(StrictRedis.pipeline, connection)
        connection._ttl = fix_return_type(partial(StrictRedis.ttl, connection))
        if hasattr(connection, 'pttl'):
            connection._pttl = fix_return_type(
                partial(StrictRedis.pttl, connection))

    # add support for mock redis objects
    elif hasattr(connection, 'setex'):
        connection._setex = connection.setex
        connection._lrem = connection.lrem
        connection._zadd = connection.zadd
        connection._pipeline = connection.pipeline
        connection._ttl = connection.ttl
        if hasattr(connection, 'pttl'):
            connection._pttl = connection.pttl
    else:
        raise ValueError("Unanticipated connection type: {}. "
                         "Please report this.".format(type(connection)))

    return connection


@contextlib.contextmanager
def Connection(connection=None):
    if connection is None:
        connection = StrictRedis()
    push_connection(connection)
    try:
        yield
    finally:
        poped = pop_connection()
        assert poped == connection, "Unexpected Redis connection was poped off"
        "the stack. Check your Redis connection setup."


def push_connection(connection):
    """Pushes the given connection on the stack"""
    _connection_stack.push(patch_connection(connection))


def pop_connection(connection=None):
    """Pops the topmost connection from the stack"""
    return _connection_stack.pop()


def use_connection(redis=None):
    """Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.
    """
    assert _connection_stack.depth() <= 1, \
        'You should not mix Connection contexts with use_connection().'
    _connection_stack.empty()

    if redis is None:
        redis = Redis()
    push_connection(redis)


def get_current_connection():
    """Return the current Redis connection"""
    return _connection_stack.top


def resolve_connection(connection=None):
    if connection is not None:
        return patch_connection(connection)

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException('Could not resolve a Redis Connection')
    return connection

__all__ = ['Connection', 'get_current_connection', 'push_connection',
           'pop_connection', 'use_connection']
