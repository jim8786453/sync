import testing.postgresql
import pytest

import sync


postgresql = testing.postgresql.Postgresql()


@pytest.fixture(scope="session")
def session_setup(request):
    def fin():
        postgresql.stop()

    request.addfinalizer(fin)


def mock_run(fun, args):
    fun(*args)


def mock_call_init_storage(system_id, create_db=False):
    pass


@pytest.fixture(autouse=True)
def no_async(monkeypatch):
    monkeypatch.setattr(sync.async, 'run', mock_run)
    monkeypatch.setattr(sync.async, '_call_init_storage',
                        mock_call_init_storage)
