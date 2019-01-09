import dbt.exceptions
from dbt.api.object import APIObject
from dbt.contracts.common import named_property
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


CONNECTION_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'type': {
            'type': 'string',
            # valid python identifiers only
            'pattern': r'^[A-Za-z_][A-Za-z0-9_]+$',
        },
        'name': {
            'type': ['null', 'string'],
        },
        'state': {
            'enum': ['init', 'open', 'closed', 'fail'],
        },
        'transaction_open': {
            'type': 'boolean',
        },
        # we can't serialize this so we can't require it as part of the
        # contract.
        # 'handle': {
        #     'type': ['null', 'object'],
        # },
        # credentials are validated separately by the adapter packages
        'credentials': {
            'description': (
                'The credentials object here should match the connection type.'
            ),
            'type': 'object',
            'additionalProperties': True,
        }
    },
    'required': [
        'type', 'name', 'state', 'transaction_open', 'credentials'
    ],
}


class Connection(APIObject):
    SCHEMA = CONNECTION_CONTRACT

    def __init__(self, credentials, *args, **kwargs):
        # we can't serialize handles
        self._handle = kwargs.pop('handle')
        if kwargs['transaction_open']:
            assert self._handle is not None
            assert self._handle.transaction is not None

        super(Connection, self).__init__(credentials=credentials.serialize(),
                                         *args, **kwargs)
        # this will validate itself in its own __init__.
        self._credentials = credentials

    @property
    def credentials(self):
        return self._credentials

    @property
    def handle(self):
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value

    name = named_property('name', 'The name of this connection')
    state = named_property('state', 'The state of the connection')
    # transaction_open = named_property(
    #     'transaction_open',
    #     'True if there is an open transaction, False otherwise.'
    # )
    @property
    def transaction_open(self):
        if 'transaction_open' not in self._contents:
            return None
        status = self._contents['transaction_open']
        if status:
            assert self.handle.transaction is not None
        elif self.handle is not None:
            assert self.handle.transaction is None
        return status

    @transaction_open.setter
    def transaction_open(self, value):
        if value or self._handle:
            assert (self.handle.transaction is not None) == value
        self._contents['transaction_open'] = value
        self.validate()

