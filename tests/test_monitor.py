from gevent import monkey; monkey.patch_all(aggressive=False)
import os
from collections import namedtuple
#from plumb_util import find_service, find_text
from syncstomp.managed import ManagedConnection
from syncstomp.conversation import SynchronousStomp
from apollo.queues import ApolloMonitor

import logging; logger = logging.getLogger(__name__)

CALL = namedtuple('Call', 'name args kwargs')

ENV_USER = os.environ.get('USER', 'jenkins')
MONITOR = None
STOMP = None
QUEUE = None

class MonitorWithEvents(ApolloMonitor):
    """
    This is the same as a normal ApolloMonitor except that its
    on_queue_* methods keep track of when they've been called.
    """

    def __init__(self, *args, **kwargs):
        self._history = []
        super(MonitorWithEvents, self).__init__(*args, **kwargs)

    def get_call_history(self):
        return self._history

    def _func(self, name, *args, **kwargs):
        logger.debug('stomp.%s(*%r, **%r)', name, args, kwargs)
        self._history.append(CALL(name, args, kwargs))

    def on_queue_init(self, *args, **kwargs):
        self._func('on_queue_init', *args, **kwargs)

    def on_queue_new(self, *args, **kwargs):
        self._func('on_queue_new', *args, **kwargs)

    def on_queue_update(self, *args, **kwargs):
        self._func('on_queue_update', *args, **kwargs)

    def on_queue_empty(self, *args, **kwargs):
        self._func('on_queue_empty', *args, **kwargs)

    def on_queue_delete(self, *args, **kwargs):
        self._func('on_queue_delete', *args, **kwargs)

def setup():
    global MONITOR, STOMP, QUEUE

#    host_and_ports = find_service('_apollo_adm._tcp', zone=None)
#    virtual_host = find_text('_apollo_vhost', zone=None)
#    host, port = host_and_ports[0]
#    host = 'ferret.lumi.'
    host = 'njord.lumi.'
    port = 61680
#    virtual_host = 'stomp-testing' # should I make a new virtual host?  how?
    virtual_host = 'broker_lumi'
    MONITOR = MonitorWithEvents(host=host, port=port, virtual_host=virtual_host,
                                update_interval_s=float('inf'))

    STOMP = ManagedConnection(host_and_ports = [(host, 61613)], version = 1.1)
    STOMP.start()
    STOMP.connect(wait = True)
    
    QUEUE = 'test.apollo.monitor.' + ENV_USER

def test_init():
    # check that on_queue_init was called for all the queues
    assert len(MONITOR._history) == len(MONITOR.queues)
    assert all(call.name == 'on_queue_init' for call in MONITOR._history)
    MONITOR._history = []

def test_queue_data():
    # _get_queue_data, _structure_queue_data

    # make a stomp listener and subscribe it to the queue
    # (just because I think that'll create the queue)
    SynchronousStomp(STOMP, destination='/queue/'+QUEUE)

    # get some queue data, and make sure our queue is in there
    queue_data = MONITOR._get_queue_data()
    ids = [x['id'] for x in queue_data]
    assert QUEUE in ids
    data = [x for x in queue_data if x['id']==QUEUE][0]
    structured = MONITOR._structure_queue_data(queue_data)
    assert structured[QUEUE] == data

def test_delete_queue():
    # delete_queue
    MONITOR.delete_queue(QUEUE)
    # make sure queue isn't there anymore
    queue_data = MONITOR._get_queue_data()
    ids = [x['id'] for x in queue_data]
    assert QUEUE not in ids

def test_queue_changes():
    # _detect_queue_changes

    # first detect stuff that has happened since we initialized
    new_data = MONITOR._get_queue_data()
    MONITOR._detect_queue_changes(MONITOR._structure_queue_data(new_data))
    relevant_calls = [c for c in MONITOR._history if c.args[0]['id']==QUEUE]
    # check that the queue was created and deleted
    assert any(c.name == 'on_queue_new' for c in relevant_calls)
    assert any(c.name == 'on_queue_delete' for c in relevant_calls)

    # TODO: figure out what should go in the remainder of this test

    # send a message
    message = {}
    STOMP.send(message, destination='/queue/'+QUEUE)
    new_data = MONITOR._get_queue_data()
    MONITOR._detect_queue_changes(MONITOR._structure_queue_data(new_data))
    # on_queue_update (which calls on_queue_empty), on_queue_new,
    # and on_queue_delete get called

def test_do_update():
    # do_update (this has to do with gevent Events)
    # TODO
    pass

def teardown():
    # I can't think of much we need to do here
    MONITOR.delete_queue(QUEUE)
