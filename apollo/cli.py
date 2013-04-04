
from gevent import monkey; monkey.patch_all()
import logging; logger = logging.getLogger(__name__)
from plumb_util import find_service, find_text
from credservice.client import CredClient

from argparse import ArgumentParser
from syncstomp import add_argparse_group, syncstomp_from_args

from .queues import ApolloMonitor
from gevent import sleep
from sys import exit

# Declare a class for inspecting queue records cleanly
import pprint
class PrettyMonitor(ApolloMonitor):
    def on_queue_init(self, queue):
        super(PrettyMonitor,self).on_queue_init(queue)
        pprint.pprint(queue,indent=1)

def monitor_from_args(args, monitor_class=ApolloMonitor):
    """
    Prepare a CredClient from ArgumentParser args
    """
    # Prepare the STOMP connection and CredClient
    stomp_connection = syncstomp_from_args(args, 'tellme')
    tellme = CredClient(stomp_connection)

    # Auto-detect Apollo administration server and credentials
    zone = args.zone
    host_and_ports = find_service('_apollo_adm._tcp', zone)
    virtual_host = find_text('_apollo_vhost', zone)
    apollo_cred = tellme('apollo-admin')

    # There really should only be one host-port pair
    if len(host_and_ports) > 1:
        logger.warn('More than one apollo administration service found')

    if len(host_and_ports) == 0:
        logger.error('Apollo admin service not found')
        exit(1)

    if virtual_host is None:
        logger.error('Apollo vhost unknown')
        exit(1)

    # Construct the monitor
    host, port = host_and_ports[0]
    monitor = monitor_class(host=host,
                            port=port,
                            virtual_host=virtual_host,
                            username=apollo_cred['username'],
                            password=apollo_cred['password'])
    return monitor

def start_monitor():
    # Prepare the argument parser
    parser = ArgumentParser(version='ApolloMonitor 0.1')
    add_argparse_group(parser)
    parser.set_defaults()
    args = parser.parse_args()

    monitor = monitor_from_args(args, PrettyMonitor)

    # Wait
    while True:
        sleep(1)
    return monitor

def purge_queues():
    """
    Purge unused queues
    """
    parser = ArgumentParser(version='Apollo Queue Purger 0.1')
    add_argparse_group(parser)
    parser.add_argument('-p', '--pattern', default='.worker',
                        help='Queue destination substring to match for purge '
                             'candidates (default: %(default)s)')
    parser.set_defaults()
    args = parser.parse_args()
    monitor = monitor_from_args(args)

    # Iterate over all queues and delete unused 'worker' queues
    for queue, dic in list(monitor.queues.items()):
        if args.pattern not in queue:
            continue
        if dic['metrics']['queue_items'] == 0:
            continue
        if (dic['metrics']['consumer_count'] + dic['metrics']['producer_count']) == 0:
            logger.warn('deleting %s', queue)
            monitor.delete_queue(queue)
