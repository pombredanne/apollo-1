
from gevent import monkey; monkey.patch_all()
import logging; logger = logging.getLogger(__name__)
from plumb_util import find_service, find_text
from credservice.client import CredClient

from argparse import ArgumentParser
from syncstomp import Connection, add_argparse_group, syncstomp_from_args

from .queues import ApolloMonitor
from gevent import sleep

def start_monitor():
    # Prepare the argument parser
    parser = ArgumentParser(version='ApolloMonitor 0.1')
    add_argparse_group(parser)
    parser.set_defaults()
    args = parser.parse_args()

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
        return

    if virtual_host is None:
        logger.error('Apollo vhost unknown')
        return

    # Construct the monitor
    host, port = host_and_ports[0]
    monitor = ApolloMonitor(host=host,
                            port=port,
                            virtual_host=virtual_host,
                            username=apollo_cred['username'],
                            password=apollo_cred['password'])

    # Wait
    while True:
        sleep(1)
