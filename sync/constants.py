

class Type(object):

    Settings = 'Settings'
    Node = 'Node'
    Message = 'Message'
    Record = 'Record'
    Change = 'Change'
    Error = 'Error'
    Remote = 'Remote'


class Method(object):

    Create = 'create'
    Read = 'read'
    Update = 'update'
    Delete = 'delete'

    All = [Create, Read, Update, Delete]


class State(object):

    Pending = 'pending'
    Processing = 'processing'
    Acknowledged = 'acknowledged'
    Failed = 'failed'


class Text(object):

    ChangeInvalid = 'Change object is not editable'
    ErrorInvalid = 'Error object is not editable'
    MessageStateInvalid = 'Can not change Message state from {0} to {1}'
    MissingPayload = 'Messages require a payload when using the Create or Update method'  # noqa
    NodeHasPendingMessages = 'Node has pending messages that must be fetched'
    NodeMissingPermission = 'Node does not have permission to {0}'
    NodeSendReadInvalid = 'Nodes may not send Read messages, only fetch them'
    NodeSendCreateInvalid = 'Nodes may not supply a record_id parameter when using the Create method'  # noqa
    NotFound = '{0} object with id:{0} not found'
    RecordNotFound = 'Expected record not found'
    RemoteOrRecordRequired = 'Either a remote_id or record_id is required.'
