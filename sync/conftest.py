import pytest
import testing.postgresql

import sync

postgresql = testing.postgresql.Postgresql(
    postgres_args='-h 127.0.0.1 -F -c logging_collector=off -c max_connections=100'  # noqa
)


@pytest.fixture(scope="session")
def session_setup(request):
    """Cleanup Postgres connections after tests have run.

    """
    def fin():
        postgresql.stop()

    request.addfinalizer(fin)


def mock_init_storage(system_id, create_db=False):
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    pass


def mock_call_close():
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    pass


class MockProcess(object):

    def __init__(self, target, args):
        self.target = target
        self.args = args

    def start(self):
        self.target(*self.args)


@pytest.fixture(autouse=True)
def no_async(monkeypatch):
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    monkeypatch.setattr(sync.tasks, 'Process', MockProcess)
    monkeypatch.setattr(sync.tasks, 'init_storage', mock_init_storage)
    monkeypatch.setattr(sync.tasks, '_call_close', mock_call_close)
