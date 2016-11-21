from multiprocessing import Process

import sync

from sync.storage import init_storage


def run(fun, args):
    p = Process(target=fun, args=args)
    p.start()


def _call_init_storage(system_id, create_db=False):
    init_storage(system_id, create_db)


def _call_close():
    sync.close()


def node_sync(system_id, node_id):
    try:
        _call_init_storage(system_id)

        node = sync.Node.get(node_id)
        for batch in sync.Record.get_all():
            for record in batch:
                remote = record.remote(node.id)
                remote_id = None
                if remote is not None:
                    remote_id = getattr(remote, 'id', None)
                sync.Message.send(None, sync.constants.Method.Create,
                                  record.head, parent_id=None,
                                  destination_id=node.id,
                                  record_id=record.id,
                                  remote_id=remote_id)
    finally:
        _call_close()


def message_propagate(system_id, message):
    try:
        _call_init_storage(system_id)

        nodes = sync.Node.get()

        for node in [n for n in nodes if n.id != message.origin_id and n.read]:
            remote = sync.Remote.get(node.id, record_id=message.record_id)
            remote_id = remote.remote_id if remote else None

            sync.Message.send(None, message.method, message.payload,
                              message.id, node.id, message.record_id,
                              remote_id)
    finally:
        _call_close()
