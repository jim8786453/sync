import sync

class Storage(object):
    """Abstract class that defines a public interface for all storage
    implementations.

    """

    def connect(self):
        """Setup a connection to the storage backend if needed."""
        raise NotImplementedError

    def disconnect(self):
        """Destroy the connection to the storage backend if needed."""
        raise NotImplementedError

    def drop(self):
        """Delete all data for the current sync network."""
        raise NotImplementedError

    def start_transaction(self):
        """If the backend supports transactions start a new transaction."""
        raise NotImplementedError

    def commit(self):
        """If the backend supports transactions, commit the current
        transaction.

        """
        raise NotImplementedError

    def rollback(self):
        """If the backend supports transactions, roll back the current
        transaction.

        """
        raise NotImplementedError

    def save_network(self, network):
        """Save a network object to the storage backend.

        :param network: Network object to save
        :type network: sync.Network
        """
        raise NotImplementedError

    def save_node(self, node):
        """Save a node object to the storage backend.

        :param node: Node object to save
        :type node: sync.Node
        """
        raise NotImplementedError

    def save_message(self, message):
        """Save a message object to the storage backend.

        :param message: Message object to save
        :type message: sync.Message
        """
        raise NotImplementedError

    def save_error(self, error):
        """Save a error object to the storage backend.

        :param error: Error object to save
        :type error: sync.Error
        """
        raise NotImplementedError

    def save_change(self, change):
        """Save a change object to the storage backend.

        :param error: Change object to save
        :type error: sync.Change
        """
        raise NotImplementedError

    def save_record(self, record):
        """Save a record object to the storage backend.

        :param error: Record object to save
        :type error: sync.Record
        """
        raise NotImplementedError

    def save_remote(self, remote):
        """Save a remote object to the storage backend.

        :param remote: Remote object to save
        :type remote: sync.Remote
        """
        raise NotImplementedError

    def get_network(self):
        """Fetch the current sync network.

        :returns: Sync object
        :rtype: sync.Network

        """
        raise NotImplementedError

    def get_node(self, node_id):
        """Fetch a node by its id.

        :param node_id: The id of the node to fetch.
        :returns: Node object.
        :rtype: sync.Node

        """
        raise NotImplementedError

    def get_record(self, record_id):
        """Fetch a record by it's id.

        :param record_id: The id of the record to fetch.
        :returns: A record object.
        :rtype: sync.Record

        """
        raise NotImplementedError

    def get_remote(self, node_id, remote_id=None, record_id=None):
        """Fetch a remote using a node id and either a remote or record id.

        :param node_id: The node id of the remote to fetch.
        :param remote_id: The remote_id of the remote to fetch.
        :param record_id: The record_id of the remote to fetch.
        :returns: A remote object
        :rtype: sync.Remote

        """
        raise NotImplementedError

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.State.Pending,
                    with_for_update=False):
        """Fetch a message object based on the keyword args.

        :param message_id: The id of the message.
        :param destination_id: The destination node id of the message.
        :param state: The current state of the node.
        :param with_for_update: True if the storage backend should
            lock the row.
        :returns: A message object.
        :rtype: sync.Message

        """
        raise NotImplementedError

    def get_message_count(self, destination_id=None, state=sync.State.Pending):
        """Fetch a count of messages.

        :param destination_id: The destination node id of the message.
        :param state: The current state of the node.
        :returns: The number of messages
        :rtype: integer

        """
        raise NotImplementedError

    def get_nodes(self):
        """Fetch all node objects in the network.

        :returns: An array of nodes.
        :rtype: array

        """
        raise NotImplementedError

    def get_records(self):
        """Fetch all records in the network.

        :returns: An iterator of record objects.
        :rtype: iterator

        """
        raise NotImplementedError

    def get_errors(self, message_id):
        """Fetch errors for a particular message.

        :param message_id: The id of the message the errors belong to.
        :returns: An array of errors.
        :rtype: array

        """
        raise NotImplementedError

    def get_changes(self, message_id):
        """Fetch changes for a particular message.

        :param message_id: The id of the message the changes belong to.
        :returns: An array of changes.
        :rtype: array

        """
        raise NotImplementedError

    def update_messages(self, node_id, record_id, remote_id):
        """Update the remote id of all messages with a matching node id and
        record id.

        :param node_id: The node id to filter by.
        :param record_id: The record id to filter by.
        :param remote_id: The remote id value to set.

        """
        raise NotImplementedError
