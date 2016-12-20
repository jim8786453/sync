from multiprocessing import Process

import sync

from sync.storage import init_storage


def run(fun, args):
    """Run a function in a seperate process.

    :param fun: The function to run.
    :type fun: function
    :param args: The arguements to apply to fun.
    :type args: tuple

    """
    p = Process(target=fun, args=args)
    p.start()


def node_sync(system_id, node_id):
    """Resend all records to a node.

    :param system_id: Unique identifier of the system.
    :type system_id: str
    :param node_id: Unique identifier of the node.
    :type node_id: str
    """
    try:
        init_storage(system_id, False)

        node = sync.Node.get(node_id)
        for batch in sync.Record.get_all():
            for record in batch:
                remote = record.remote(node.id)
                remote_id = None
                if remote is not None:
                    remote_id = getattr(remote, 'remote_id', None)
                sync.Message.send(None, sync.constants.Method.Create,
                                  record.head, parent_id=None,
                                  destination_id=node.id,
                                  record_id=record.id,
                                  remote_id=remote_id)
    finally:
        sync.close()


def message_propagate(system_id, message):
    """Propagate a message to all the other nodes in the system.

    :param system_id: Unique identifier of the system.
    :type system_id: str
    :param message_id: Unique identifier of the message.
    :type message_id: str

    """
    try:
        init_storage(system_id, False)

        nodes = sync.Node.get()

        for node in [n for n in nodes if n.id != message.origin_id and n.read]:
            remote = sync.Remote.get(node.id, record_id=message.record_id)
            remote_id = remote.remote_id if remote else None

            sync.Message.send(None, message.method, message.payload,
                              message.id, node.id, message.record_id,
                              remote_id)
    finally:
        sync.close()
