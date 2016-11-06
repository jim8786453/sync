import sync

from sync.storage import init_storage


def node_sync(system_id, node_id):
    init_storage(system_id, create_db=False)

    node = sync.Node.get(node_id)
    for batch in sync.Record.get_all():
        for record in batch:
            remote_id = record.remote(node.id)
            sync.Message.send(None, sync.contants.Method.Create,
                              record.head, parent_id=None,
                              destination_id=node.id,
                              record_id=record.id,
                              remote_id=remote_id)


def message_propagate(system_id, message):
    init_storage(system_id, create_db=False)

    nodes = sync.Node.get()

    for node in [n for n in nodes if n.id != message.origin_id and n.read]:
        remote = sync.Remote.get(node.id, record_id=message.record_id)
        remote_id = remote.remote_id if remote else None

        sync.Message.send(None, message.method, message.payload,
                          message.id, node.id, message.record_id,
                          remote_id)
