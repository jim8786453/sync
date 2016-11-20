import testing.postgresql
import pytest

import sync

postgresql = testing.postgresql.Postgresql(
    postgres_args='-h 127.0.0.1 -F -c logging_collector=off -c max_connections=100'  # noqa
)


@pytest.fixture(scope="session")
def session_setup(request):
    def fin():
        postgresql.stop()

    request.addfinalizer(fin)


def mock_run(fun, args):
    fun(*args)


def mock_call_init_storage(system_id, create_db=False):
    pass


def mock_call_close():
    pass


@pytest.fixture(autouse=True)
def no_async(monkeypatch):
    monkeypatch.setattr(sync.async, 'run', mock_run)
    monkeypatch.setattr(sync.async, '_call_init_storage',
                        mock_call_init_storage)
    monkeypatch.setattr(sync.async, '_call_close',
                        mock_call_close)
