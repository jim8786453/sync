import copy
import datetime
import jsonschema
import six
import uuid


from sync import exceptions, async
from sync.constants import Method, State, Text, Type


s = None


def init(storage):
    """Set the global storage object used to persist data.

    :param storage: Global storage object.
    :type storage: sync.Storage.Storage

    """
    global s
    s = storage


def generate_id():
    """Generate a globally unique identifier.

    :returns: str - A unique identifier.
    :rtype: str.

    """
    id_ = uuid.uuid4()
    return str(id_)


def merge_patch(target, patch):
    """Patch a dictionary.

    Recursively update a dict with a second dict following the JSON
    merge patch guidelines https://tools.ietf.org/html/rfc7396.

    :param target: The dict to update.
    :type target: dict
    :param patch: The values to apply to the target.
    :type patch: dict

    """
    if not isinstance(target, dict):
        target = {}
    if isinstance(patch, dict):
        for name, value in six.iteritems(patch):
            if value is None:
                if name in target:
                    del target[name]
            else:
                target[name] = merge_patch(target.get(name, {}), value)
        return target
    else:
        return patch


class Base(object):
    """Base class for all model classes to inherit from.

    Provides generic equality methods for objects of this class
    (__eq__ and __ne__) as well as helper utilities.

    """

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def as_dict(self, with_id=False):
        """Return the object as a dictionary.

        :param with_id: A true value incdicates that the id property
            should be included in the return value.
        :type with_id: bool

        :returns: A deepcopy of self.__dict__
        :rtype: dict

        """
        result = copy.deepcopy(self.__dict__)
        if not with_id:
            del result['id']

        for key in list(result):
            if key.startswith('_'):
                del result[key]

        return result


class System(Base):
    """Holds configuration and settings for the sync system. Only one
    instance of this class is ever in use.

    """

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: name (str): User friendly identifier.
        self.name = None
        #: schema (dict): JSON schema definition used to validate
        # sync.Record data.
        self.schema = None
        #: fetch_before_send (bool): Determines whether nodes must
        # fetch all pending messages before they may send a message.
        self.fetch_before_send = True

    def save(self):
        """Save the object using the global sync.Storage object."""
        s.start_transaction()
        s.save_system(self)
        s.commit()

    @staticmethod
    def get():
        """Fetch the object using the global sync.Storage object."""
        return s.get_system()

    @staticmethod
    def init(name, schema, fetch_before_send=True):
        """Upserts the system.

        :param name: Friendly name for the sync system.
        :type name: str
        :param schema: JSON schema definition used to validate record
            data.
        :type schema: dict
        :param fetch_before_send: Determines whether nodes must fetch
            all pending messages before they may send a message.
        :type fetch_before_send: bool
        :returns: Instantiated system object.
        :rtype: sync.System

        """
        system = s.get_system()
        if system is None:
            system = System()
        system.name = name
        system.schema = schema
        system.fetch_before_send = fetch_before_send
        system.save()
        return system


class Node(Base):
    """Nodes are external systems that want to sync data with other nodes.

    """

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: name (str): User friendly identifier.
        self.name = None
        #: create (bool): Can the node create records.
        self.create = False
        #: read (bool): Can the node read records.
        self.read = False
        #: update (bool): Can the node update records.
        self.update = False
        #: delete (bool): Can the node delete records.
        self.delete = False

    def _get_message(self, message_id):
        """Helper function to fetch a message based on it's id.

        :param message_id: The id of the message to fetch.
        :type message_id: str
        :returns: Message object
        :rtype: sync.Message
        :raises: sync.exceptions.NotFoundError

        """
        message = Message.get(message_id)

        if message is None:
            raise exceptions.NotFoundError(Type.Message, message_id)

        return message

    def save(self):
        """Save the object using the global sync.Storage object."""
        s.start_transaction()
        s.save_node(self)
        s.commit()

    def send(self, method, payload=None, record_id=None,
             remote_id=None):
        """Send a message.

        This method sends a message with the messages origin set to
        the current node. The message destination is set to None
        indicating this message will be processed by the sync system.

        :param method: Create, update or delete.
        :type method: sync.constants.Method
        :param payload: Data required when creating or updating the record.
        :type payload: dict
        :param record_id: The record id when updating or deleting a record.
        :type record_id: str
        :param remote_id: A node specific identifier to associate with a
            record.
        :type remote_id: str
        :returns: Message object.
        :rtype: sync.Message
        :raises: sync.exceptions.InvalidOperationError

        """
        if method == Method.Read:
            raise exceptions.InvalidOperationError(Text.NodeSendReadInvalid)

        if method == Method.Create and record_id is not None:
            raise exceptions.InvalidOperationError(Text.NodeSendCreateInvalid)

        return Message.send(self.id, method, payload,
                            parent_id=None, destination_id=None,
                            record_id=record_id, remote_id=remote_id)

    def fetch(self):
        """Fetch the next available, pending message object for the current
        node.

        :returns: Message object.
        :rtype: sync.Message

        """
        return Message.fetch(self.id)

    def acknowledge(self, message_id, remote_id=None):
        """Acknowledge a message.

        Acknowledge that a message has been successfully
        processed. Optionally supply a node specific identifier to
        associate with the record the message affected.

        :param message_id: Id of the message to acknowledge.
        :type message_id: str


        :param remote_id: A node specific identifier to associate with a
            record.
        :type remote_id: str
        :returns: The acknowledged message object.
        :rtype: sync.Message

        """
        message = self._get_message(message_id)
        message.acknowledge(remote_id)
        return message

    def fail(self, message_id, reason=""):
        """Fail a message.

        Signal that processing a message has failed. Optionally supply
        a reason why processing failed.

        :param message_id: Id of the message to acknowledge.
        :type message_id: str
        :param reason: Description of why processing a message has failed.
        :type remote_id: str
        :returns: The failed message object.
        :rtype: sync.Message

        """
        message = self._get_message(message_id)
        message.fail(reason)
        return message

    def sync(self):
        """Resend all records to the current node.

        :returns: A message object
        :rtype: sync.Message

        """
        args = (s.id, self.id)
        async.run(async.node_sync, args)

    def check(self, method):
        """Verify the node has permission to use a method.

        :param method: The method the node would like to use.
        :type method: sync.constants.Method
        :returns: True if the node has permission to use the method.
        :rtype: bool

        """
        if method == Method.Create and self.create:
            return True
        elif method == Method.Read and self.read:
            return True
        elif method == Method.Update and self.update:
            return True
        elif method == Method.Delete and self.delete:
            return True
        return False

    def disable(self):
        self.create = False
        self.read = False
        self.update = False
        self.delete = False
        self.save()

    @staticmethod
    def get(node_id=None):
        """Fetch the object using the global sync.Storage object.

        :param node_id: The id of the node to fetch.
        :type node_id: str
        :returns: Node object.
        :rtype: sync.Node

        """
        if node_id is None:
            return s.get_nodes()
        return s.get_node(node_id)

    @staticmethod
    def create(name=None, create=False, read=False, update=False,
               delete=False):
        """Create a node.

        :param name: User friendly identifier.
        :type name: str
        :param create: Can the node create records.
        :type create: bool
        :param read: Can the node read records.
        :type read: bool
        :param update: Can the node update records.
        :type update: bool
        :param delete: Can the node delete records.
        :type delete: bool
        :returns: The new node object.
        :rtype: sync.Node
        """
        node = Node()
        node.name = name
        node.create = create
        node.read = read
        node.update = update
        node.delete = delete
        node.save()
        return node


class Message(Base):
    """Message are the core object in sync used to update records."""

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: parent_id (str): Id of parent message.
        self.parent_id = None
        #: origin_id (str): Id of origin node.
        self.origin_id = None
        #: destination_id (str): Id of the destination node.
        self.destination_id = None
        #: timestamp (datetime): When the message was created.
        self.timestamp = datetime.datetime.utcnow()
        #: method (sync.constants.Method): Create, read, update or delete.
        self.method = Method.Create
        #: payload (dict): The data to use for create, read or updates.
        self.payload = None
        #: remote_id (str): A node specific identifier to associate
        #: with a record.
        self.remote_id = None
        #: record_id (str): The record id when updating or deleting a record.
        self.record_id = None
        #: state (sync.constants.State): Current message state.
        self.state = State.Pending

        # Cache commonly required objects.
        self._system = None
        self._parent = None
        self._origin = None
        self._destination = None
        self._remote = None
        self._record = None

    def _execute(self):
        """Apply the message to the record store."""
        if self._record is None:
            self._record = Record()

        existing = self._record.head if self._record.head is not None else {}

        if self.method == Method.Delete:
            self._record.head = None
            self._record.deleted = True
        else:
            self._record.head = merge_patch(existing, self.payload)

        self._record.validate()
        self._record.save()

        self.record_id = self._record.id
        self.save()

    def _inflate(self):
        """Fetch objects related to this message."""
        self._system = s.get_system()
        self._parent = s.get_message(self.parent_id)
        self._origin = s.get_node(self.origin_id)
        self._destination = s.get_node(self.destination_id)
        self._record = s.get_record(self.record_id)
        if self.remote_id is not None:
            self._remote = s.get_remote(
                self.origin_id, remote_id=self.remote_id)
        if self._record is None and self._remote is not None:
            self._record = s.get_record(self._remote.record_id)

    def _propagate(self):
        """Forward the message to all other nodes that have the read
        permission.

        """
        args = (s.id, self)
        async.run(async.message_propagate, args=args)

    def _validate(self):
        """Validate that the message is in a state that can be processed."""
        if self.parent_id is not None and self._parent is None:
            raise exceptions.NotFoundError(Type.Message, self.parent_id)

        if self.origin_id is not None and self._origin is None:
            raise exceptions.NotFoundError(Type.Node, self.origin_id)

        if self.destination_id is not None and self._destination is None:
            raise exceptions.NotFoundError(Type.Node, self.destination_id)

        if self.method in (Method.Create, Method.Update) \
           and self.payload is None:
            raise exceptions.InvalidOperationError(Text.MissingPayload)

        if self.method != Method.Create and self._record is None:
            raise exceptions.InvalidOperationError(Text.RecordNotFound)

        if self._system.fetch_before_send and self._origin and \
           s.get_message(destination_id=self.origin_id) is not None:
            raise exceptions.InvalidOperationError(Text.NodeHasPendingMessages)

        if self._origin and not self._origin.check(self.method):
            text = Text.NodeMissingPermission.format(self.method)
            raise exceptions.InvalidOperationError(text)

    def save(self):
        """Save the object using the global sync.Storage object."""
        s.save_message(self)

    def update(self, value, note=""):
        """Update the state of a message.

        Validates the new message state, updates the message and
        creates a new sync.Change object.

        :param value: The new message state.
        :type value: sync.constants.State
        :param note: Text description of the change.
        :type note: str

        """
        assert self.id is not None

        if value in (State.Pending, State.Acknowledged, State.Failed) and \
           self.state != State.Processing:
            text = Text.MessageStateInvalid.format(
                self.state, value)
            raise exceptions.InvalidOperationError(text)

        self.state = value

        change = Change()
        change.message_id = self.id
        change.state = value
        change.note = note

        change.save()
        self.save()

    def acknowledge(self, remote_id=None):
        """Acknowledge the message.

        Acknowledge that a message has been successfully
        processed. Optionally supply a node specific identifier to
        associate with the record the message affected.

        :param remote_id: A node specific identifier to associate with a
            record.
        :type remote_id: str
        :returns: The acknowledged message object.
        :rtype: sync.Message

        """
        assert self.id is not None
        assert self.state == State.Processing

        s.start_transaction()
        self._inflate()
        self.update(State.Acknowledged)

        if self.destination_id is not None and remote_id is not None \
           and self.remote_id != remote_id:
            assert self.record_id is not None

            Remote.create(self.destination_id, self.record_id,
                          remote_id)
        s.commit()

    def fail(self, reason=""):
        """Fail the message.

        Signal that processing the message has failed. Optionally supply
        a reason why processing failed.

        :param reason: Description of why processing a message has failed.
        :type remote_id: str
        :returns: The failed message object.
        :rtype: sync.Message

        """
        assert self.id is not None
        assert self.state == State.Processing

        s.start_transaction()
        self.update(State.Failed)

        if reason:
            error = Error()
            error.message_id = self.id
            error.text = reason
            error.save()

        s.commit()

    def errors(self):
        """Fetch the messages errors using the global sync.Storage object."""
        if self.id is None:
            return []
        return s.get_errors(self.id)

    def changes(self):
        """Fetch the messages changes using the global sync.Storage object."""
        if self.id is None:
            return []
        return s.get_changes(self.id)

    @staticmethod
    def get(message_id):
        """Fetch a message using the global sync.Storage object.

        :param message_id: The id of the message to fetch.
        :type message_id: str
        :returns: A message object
        :rtype: sync.Message

        """
        return s.get_message(message_id)

    @staticmethod
    def send(origin_id, method, payload=None, parent_id=None,
             destination_id=None, record_id=None, remote_id=None):
        """Send a message.

        :param origin_id: The origin node id.
        :type origin_id: str
        :param method: Create, read, update or delete.
        :type method: sync.constants.Method
        :param payload: The data to use for create, read or updates.
        :type payload: dict
        :param parent_id: Id of parent message.
        :type parent_id: str
        :param destination_id: Id of the destination node.
        :type destination_id: str
        :param record_id: The record id when updating or deleting a record.
        :type record_id: str
        :param remote_id: A node specific identifier to associate with
            a record.
        :type remote_id: str
        :returns: The sent message object
        :rtype: sync.Message

        """
        message = Message()

        message.parent_id = parent_id
        message.origin_id = origin_id
        message.destination_id = destination_id
        message.method = method
        message.payload = payload
        message.record_id = record_id
        message.remote_id = remote_id

        try:
            s.start_transaction()
            message._inflate()
            message._validate()
            message.save()
            s.commit()
        except Exception:
            s.rollback()

            raise

        if destination_id is not None:
            return message

        s.start_transaction()
        message.update(State.Processing)
        s.commit()

        try:
            s.start_transaction()
            message._execute()
            message._propagate()
            message.update(State.Acknowledged)
            s.commit()
        except Exception:
            s.rollback()
            s.start_transaction()
            message.update(State.Failed)
            s.commit()

            raise

        return message

    @staticmethod
    def fetch(destination_id):
        """Fetch next pending message.

        This method returns the next message that is in a pending
        state and has a matching destination_id property.

        Returns None if no message can be found in this state.

        :param destination_id: Destination node id.
        :type destination_id: str
        :returns: Message object, with its state updated to processing.
        :rtype: sync.Message

        """
        try:
            s.start_transaction()
            message = s.get_message(destination_id=destination_id,
                                    with_for_update=True)
            if message is None:
                return None
            message.update(State.Processing)
            s.commit()
            return message
        except Exception:
            s.rollback()

            raise


class Error(Base):
    """Tracks errors raised when processing messages."""

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: message_id (str): Unique id of the message this error belongs to.
        self.message_id = None
        #: timestamp (datetime.datetime): When the error occurred.
        self.timestamp = datetime.datetime.utcnow()
        #: text (str): Textual description of the error.
        self.text = None

    def save(self):
        """Save the object using the global sync.Storage object."""
        if self.id is not None:
            raise exceptions.InvalidOperationError(Text.ErrorInvalid)
        s.save_error(self)


class Change(Base):
    """Track changes to message state."""

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: message_id (str): Unique id of the message this change belongs to.
        self.message_id = None
        #: timestamp (datetime.datetime): When the change occurred.
        self.timestamp = datetime.datetime.utcnow()
        #: state (sync.constants.State): The new message state.
        self.state = None
        #: text (str): Textual description of the change.
        self.note = None

    def save(self):
        """Save the object using the global sync.Storage object."""
        if self.id is not None:
            raise exceptions.InvalidOperationError(Text.ChangeInvalid)
        s.save_change(self)


class Record(Base):
    """A record of data to be synced."""

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: last_updated (datetime.datetime): When the last change to
        #: the record was made.
        self.last_updated = datetime.datetime.utcnow()
        #: deleted (bool): Has the record been deleted.
        self.deleted = False
        #: head (dict): The current state of the record.
        self.head = None
        #: remotes (list): Cache of sync.Remote objects for this record.
        self.remotes = []

    def validate(self):
        """Validate the record head against the current JSON schema.

        :returns: True is the record head is valid.
        :rtype: bool

        """
        if self.deleted and self.head is None:
            return True

        system = s.get_system()
        jsonschema.validators.Draft4Validator(
            system.schema).validate(self.head)

        return True

    def save(self):
        """Save the object using the global sync.Storage object."""
        self.last_updated = datetime.datetime.utcnow()
        s.save_record(self)

    def remote(self, node_id):
        """If one exists, fetch the nodes remote object for this record.

        :param node_id: The id of the node the remote belongs to.
        :type node_id: str

        """
        for r in self.remotes:
            if r.node_id == node_id:
                return r
        return None

    @staticmethod
    def get(record_id):
        """Fetch a record object by its id using the global sync.Storage
        object.

        :param record_id: The id of the record to fetch.
        :returns: A record object.
        :rtype: sync.Record

        """
        return s.get_record(record_id)

    @staticmethod
    def get_all():
        """Fetch all records in batches.

        :returns: Record batched using a generator.
        :rtype: generator

        """
        return s.get_records()


class Remote(Base):
    """Remotes link records to nodes using a unique id provided by the
    node itself.

    This means that a node can sync data without keeping track or
    storing the sync systems record ids. Instead it can opt to provide
    it's own unique id for each record.

    """

    def __init__(self):
        #: id (str): Unique identifier.
        self.id = None
        #: node_id (str): The id of the node.
        self.node_id = None
        #: record_id (str): The id of the record.
        self.record_id = None
        #: remote_id (str): The unique id provided by the node.
        self.remote_id = None

    def save(self):
        """Save the object using the global sync.Storage object."""
        s.save_remote(self)

    @staticmethod
    def get(node_id, remote_id=None, record_id=None):
        """Fetch the object using the global sync.Storage object.

        :param node_id: The id of the node.
        :param remote_id: The unique id provided by the node.
        :param record_id: The id of the record.
        :returns: Remote object
        :rtype: sync.Remote

        """
        return s.get_remote(node_id, remote_id, record_id)

    @staticmethod
    def create(node_id, record_id, remote_id):
        """Create a remote.

        :param node_id: The id of the node.
        :param record_id: The id of the record.
        :param remote_id: The unique id provided by the node.
        :returns: A remote object
        :rtype: sync.Remote

        """
        remote = Remote()
        remote.node_id = node_id
        remote.record_id = record_id
        remote.remote_id = remote_id
        remote.save()

        s.update_messages(node_id, record_id, remote_id)

        return remote
