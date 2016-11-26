import testing.postgresql
import pytest

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


def mock_run(fun, args):
    """Apply args to a function.

    """
    fun(*args)


def mock_call_init_storage(system_id, create_db=False):
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    pass


def mock_call_close():
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    pass


@pytest.fixture(autouse=True)
def no_async(monkeypatch):
    """Mock this functionality as unit tests do not run tasks in seperate
    processes so results can more easily be verified.

    """
    monkeypatch.setattr(sync.tasks, 'run', mock_run)
    monkeypatch.setattr(sync.tasks, '_call_init_storage',
                        mock_call_init_storage)
    monkeypatch.setattr(sync.tasks, '_call_close',
                        mock_call_close)
