
import logging; logger = logging.getLogger(__name__)
from requests1 import Session
from credservice.utils import call_periodic
from gevent.event import Event

# We don't need to see the queue update every five seconds.
logging.getLogger('requests').setLevel(logging.WARN)

class ApolloMonitor(object):
    """Class for monitoring an Apache Apollo server"""
    def __init__(self, host, virtual_host, port=61680,
                 realm='Apollo', username='admin', password='password',
                 update_interval_s=5):
        """Construct a new ApolloMonitor that monitors the $virtual_host
           virtual-host on the Apollo server at $host:$port with credentials
           $username and $password.  Monitor for update events every
           $update_interval_s seconds."""
        # Prepare a URL opener
        self.auth = (username, password)
        self._url = ('http://%s:%d/broker/virtual-hosts/%s'
                     % (host, port, virtual_host))
        self._url_queues = self._url + '/queues.json'
        self._url_delete = self._url + '/queues/%s.json'
        self._s = Session()

        # Initialize the queue status dictionary
        self.queues = self._structure_queue_data(self._get_queue_data())
        for queue in self.queues.values():
            self.on_queue_init(queue)

        # Initialize the update wait event
        self.update_event = Event()
        self.update_event.clear()

        # Run updates in a loop
        call_periodic(update_interval_s, self.do_update)

    def _get_queue_data(self):
        """Return a parsed structure containing the current queue data"""
        # Repeat until a full download is accomplished
        page_size = -1
        total_rows = 0
        while page_size < total_rows:
            # Determine the new page size
            page_size = total_rows + 1000
            url = self._url_queues + ('?ps=%d' % page_size)

            # Get the JSON-formatted data
            queues = self._s.get(url, auth=self.auth).json
            if callable(queues):
                queues = queues()

            # Extract the new page size and row counts
            page_size = queues['page_size']
            total_rows = queues['total_rows']

        # Operation Complete!
        return queues['rows']

    def _structure_queue_data(self, queues, exclude_temp=True):
        """Construct a dictionary mapping destination names to a queue data
           structure, optionally excluding temporary destinations."""
        return dict((q['id'], q)
                    for q in queues
                    if not exclude_temp or not q['id'].startswith('temp.'))

    def _detect_queue_changes(self, new_queues):
        """Fire events for handling new, updated, and deleted queues"""

        # We will send a blank logging message if there is at least one event
        any_events = False

        # Keep a list of the old queues
        old_queues = set(self.queues.keys())

        # Iterate over new_queues
        for q_id in new_queues:
            queue = new_queues[q_id]

            # Detect a modified queue
            if q_id in old_queues:
                # Report the update
                self.on_queue_update(self.queues[q_id], queue)
                old_queues.remove(q_id)
                any_events |= True
            else:
                # Report the new queue
                self.on_queue_new(queue)
                any_events |= True

            self.queues[q_id] = queue

        # Delete old queues
        for q_id in old_queues:
            # Report the removal
            self.on_queue_delete(self.queues[q_id])
            self.queues.pop(q_id)
            any_events |= True

        # Send a blank logging message if there were any events
        # (This causes logging output to appear in stanzas)
        if any_events:
            logger.debug('')

    def do_update(self):
        """Download new queue data and send update notifications"""
        new_queues = self._structure_queue_data(self._get_queue_data())
        self._detect_queue_changes(new_queues)

        # Report update event to blockers
        self.update_event.set()
        self.update_event.clear()

    def on_queue_init(self, queue):
        """MAY override: called after the ApolloMonitor is initializing and
           loading in the initial queue status"""
        logger.debug('on_queue_init( "%s" )' % queue['id'])
        # logger.debug('on_queue_init( %s )' % repr(queue))

    def on_queue_new(self, queue):
        """MAY override: called before a new queue is added to the status
           dictionary"""
        logger.debug('on_queue_new( "%s" )' % queue['id'])
        # logger.debug('on_queue_new( %s )' % repr(queue))

    def on_queue_update(self, old_queue, new_queue):
        """MAY override: called before a queue is updated in the status
           dictionary. Overrides MUST call the super of this event handler so
           that on_queue_empty events may be fired."""
        logger.debug('on_queue_update( "%s", ... ): %d items'
                     % (old_queue['id'], old_queue['metrics']['queue_items']))
        # logger.debug('on_queue_update( %s, %s )'
        #              % (repr(old_queue), repr(new_queue)))

        # if the queue is now empty, and something has been dequeued since
        # the last queue update, then it qualifies as "this is now empty"
        if ((new_queue['metrics']['queue_items'] == 0) and
            (old_queue['metrics']['dequeue_item_counter'] !=
             new_queue['metrics']['dequeue_item_counter']
             )):
            self.on_queue_empty(new_queue)

    def on_queue_empty(self, queue):
        """MAY override: called before a queue is update in the status
           dictionary when the queue is newly empty."""
        logger.debug('on_queue_empty( "%s" )' % queue['id'])
        # logger.debug('on_queue_empty( %s )' % repr(queue))

    def on_queue_delete(self, old_queue):
        """MAY override: called before a queue is deleted from the status
           dictionary"""
        logger.debug('on_queue_delete( "%s" )' % old_queue['id'])
        # logger.debug('on_queue_delete( %s )' % repr(old_queue))

    def delete_queue(self, queue):
        """
        Delete a given queue. Returns the status code of the request (likely
        either 204 on success or 404 if the queue doesn't exist).
        """
        # Quote properly, or will this suffice?
        queue = queue.replace('%', '%25')
        self._s.delete(self._url_delete % queue, auth=self.auth)

    def delete(self, destination):
        """
        Delete a given destination (queue, topic, or dsub).
        """
        # mostly copied from delete_queue
        destination = destination.replace('%', '%25')
        empty, thing, dest = destination.split('/')
        self._s.delete(self._url + ('/%ss/%s.json' % (thing, dest)),
                        auth=self.auth)

    def wait_for_update(self, n=1):
        """
        Wait for n updates to be fetched and sent through event handlers
        """
        for it in xrange(n):
            self.update_event.wait()


class OneQueueApolloMonitor(object):
    """Class for monitoring a single queue on an Apache Apollo server"""
    def __init__(self, queue, host, virtual_host, port=61680,
                 realm='Apollo', username='admin', password='password',
                 update_interval_s=5):
        """Construct a new ApolloMonitor that monitors the $virtual_host
           virtual-host on the Apollo server at $host:$port with credentials
           $username and $password, for the specific queue $queue. Monitor for
           update events every $update_interval_s seconds."""
        # Prepare a URL opener
        self.auth = (username, password)
        self._url = ('http://%s:%d/broker/virtual-hosts/%s'
                     % (host, port, virtual_host))
        self._url_queue = '%s/queues/%s.json' % (self._url, queue)
        self._s = Session()

        self.queue_name = queue
        self.queue = self._get_queue_data()
        if self.queue:
            self.on_queue_init()
        else:
            self.on_queue_missing()

        # Initialize the update wait event
        self.update_event = Event()
        self.update_event.clear()

        # Run updates in a loop
        self.interval = update_interval_s
        self.updater = None
        self.start_updating()

    def _get_queue_data(self):
        """Return a dictionary representing the queue"""
        queue = self._s.get(self._url_queue, auth=self.auth)
        if queue.status_code == 404:
            return None
        queue = queue.json
        if callable(queue):
            queue = queue()
        return queue

    def stop_updating(self):
        if self.updater is not None:
            self.updater.kill()
            self.updater = None

    def start_updating(self):
        if self.updater is None:
            self.updater = call_periodic(self.interval, self.do_update)

    def do_update(self):
        """Download new queue data and send update notifications"""
        new_queue = self._get_queue_data()
        if new_queue == None:
            self.on_queue_missing()
        else:
            self.on_queue_update(new_queue)

        self.queue = new_queue
        # Report update event to blockers
        self.update_event.set()
        self.update_event.clear()

    def on_queue_init(self):
        """MAY override: called after the ApolloMonitor is initializing and
           loading in the initial queue status"""
        logger.debug('on_queue_init( "%s" )' % self.queue_name)

    def on_queue_update(self, new_queue):
        """MAY override: called before the queue is updated. Overrides MUST
           call the super of this event handler so that on_queue_empty events
           may be fired."""
        old_queue = self.queue
        if old_queue is None:
            return
        logger.debug('on_queue_update( "%s", ... ): %d items'
                     % (old_queue['id'], old_queue['metrics']['queue_items']))

        # if the queue is now empty, and something has been dequeued since
        # the last queue update, then it qualifies as "this is now empty"
        if ((new_queue['metrics']['queue_items'] == 0) and
            (old_queue['metrics']['dequeue_item_counter'] !=
             new_queue['metrics']['dequeue_item_counter']
             )):
            self.on_queue_empty(new_queue)

    def on_queue_empty(self, queue):
        """MAY override: called before a queue is update in the status
           dictionary when the queue is newly empty."""
        logger.debug('on_queue_empty( "%s" )' % queue['id'])

    def on_queue_missing(self):
        """MAY override: called before a queue is update in the status
           dictionary when the queue is newly empty."""
        logger.debug('on_queue_missing( "%s" )' % self.queue_name)

    def wait_for_update(self, n=1):
        """
        Wait for n updates to be fetched and sent through event handlers
        """
        for it in xrange(n):
            self.update_event.wait()
