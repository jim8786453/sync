from multiprocessing import Process

import sync

from sync.settings import STORAGE_CLASS
from sync.storage import init_storage


def run(fun, args):
    """Run a function in a seperate process.

    :param fun: The function to run.
    :type fun: function
    :param args: The arguements to apply to fun.
    :type args: tuple

    """
    if STORAGE_CLASS == 'MockStorage':
        # In memory storage can not be shared between processes.
        fun(*args)
        return
    p = Process(target=fun, args=args)
    p.start()


def _call_close():
    """When running under unit tests a separate process is not used and
    this is mocked.

    """
    if STORAGE_CLASS == 'MockStorage':
        # Do not need to close as a new process was not spawned during
        # run.
        return
    sync.close()


def node_sync(network_id, node_id):
    """Resend all records to a node.

    :param network_id: Unique identifier of the network.
    :type network_id: str
    :param node_id: Unique identifier of the node.
    :type node_id: str
    """
    try:
        init_storage(network_id, False)

        node = sync.Node.get(node_id)
        for batch in sync.Record.get_all():
            for record in batch:
                remote = record.remote(node.id)
                remote_id = None
                if remote is not None:
                    remote_id = getattr(remote, 'remote_id', None)
                sync.Message.send(None, sync.Method.Create,
                                  record.head, parent_id=None,
                                  destination_id=node.id,
                                  record_id=record.id,
                                  remote_id=remote_id)
    finally:
        _call_close()


def message_propagate(network_id, message):
    """Propagate a message to all the other nodes in the network.

    :param network_id: Unique identifier of the network.
    :type network_id: str
    :param message_id: Unique identifier of the message.
    :type message_id: str

    """
    try:
        init_storage(network_id, False)

        nodes = sync.Node.get()

        for node in [n for n in nodes if n.id != message.origin_id and n.read]:
            remote = sync.Remote.get(node.id, record_id=message.record_id)
            remote_id = remote.remote_id if remote else None

            sync.Message.send(None, message.method, message.payload,
                              message.id, node.id, message.record_id,
                              remote_id)
    finally:
        _call_close()
