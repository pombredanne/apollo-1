from gevent import monkey; monkey.patch_all()
import os
from collections import namedtuple
#from plumb_util import find_service, find_text
from syncstomp.managed import ManagedConnection
from apollo.queues import OneQueueApolloMonitor, ApolloMonitor

import logging; logger = logging.getLogger(__name__)
from uuid import uuid4

CALL = namedtuple('Call', 'name args kwargs')

ENV_USER = os.environ.get('USER', 'jenkins')
MONITOR = None
MONITOR_ARGS = None
STOMP = None
QUEUE = None


class OneMonitorWithEvents(OneQueueApolloMonitor):
    """
    This is the same as a normal OneQueueApolloMonitor except that its
    on_queue_* methods keep track of when they've been called.
    """

    def __init__(self, *args, **kwargs):
        self._history = []
        super(OneMonitorWithEvents, self).__init__(*args, **kwargs)

    def get_call_history(self):
        return self._history

    def _func(self, name, *args, **kwargs):
        logger.debug('stomp.%s(*%r, **%r)', name, args, kwargs)
        self._history.append(CALL(name, args, kwargs))

    def on_queue_init(self, *args, **kwargs):
        self._func('on_queue_init', *args, **kwargs)

    def on_queue_update(self, *args, **kwargs):
        self._func('on_queue_update', *args, **kwargs)

    def on_queue_empty(self, *args, **kwargs):
        self._func('on_queue_empty', *args, **kwargs)

    def on_queue_missing(self, *args, **kwargs):
        self._func('on_queue_missing', *args, **kwargs)


def setup():
    global MONITOR_ARGS, STOMP, QUEUE

    # TODO: this should presumably be tested on ferret, not njord,
    #   but the ManagedConnection doesn't want to connect.

#    host_and_ports = find_service('_apollo_adm._tcp', zone=None)
#    virtual_host = find_text('_apollo_vhost', zone=None)
#    host, port = host_and_ports[0]
#    host = 'ferret.lumi.'
#    virtual_host = 'stomp-testing' # should I make a new virtual host?  how?
    host = 'njord.lumi.'
    port = 61680
    virtual_host = 'broker_lumi'
    MONITOR_ARGS = dict(host=host, port=port, virtual_host=virtual_host)
    QUEUE = 'test.apollo.onemonitor.%s.%s' % (ENV_USER, uuid4())

    STOMP = ManagedConnection(host_and_ports = [(host, 61613)], version = 1.1)
    STOMP.start()
    STOMP.connect(wait = True)
    

def test_no_queue_data():
    monitor = OneMonitorWithEvents(
        QUEUE, update_interval_s=float('inf'), **MONITOR_ARGS)
    queue_data = monitor.queue
    assert queue_data is None, queue_data
    
    # check that on_queue_missing was called
    assert len(monitor._history) == 1, monitor._history
    assert monitor._history[0].name == 'on_queue_missing', monitor._history[0]
    monitor._history = []


def test_proper_init():
    global MONITOR

    # send a message to the stomp queue
    STOMP.send({}, destination='/queue/'+QUEUE)

    MONITOR = OneMonitorWithEvents(
        QUEUE, update_interval_s=float('inf'), **MONITOR_ARGS)
    queue_data = MONITOR.queue
    assert queue_data is not None, queue_data
    
    # check that on_queue_init was called
    assert len(MONITOR._history) == 1, MONITOR._history
    assert MONITOR._history[0].name == 'on_queue_init', MONITOR._history[0]
    MONITOR._history = []


def test_queue_data():
    # get some queue data, and make sure our queue is in there
    queue_data = MONITOR._get_queue_data()
    assert queue_data['id'] == QUEUE, queue_data


def test_queue_changes():
    MONITOR.do_update()
    # check that the queue was created and deleted
    assert any(c.name == 'on_queue_update' for c in MONITOR._history)


def test_do_update():
    # do_update (this has to do with gevent Events)
    # TODO
    pass

def teardown():
    # I can't think of much we need to do here
    monitor = ApolloMonitor(**MONITOR_ARGS)
    monitor.delete_queue(QUEUE)
