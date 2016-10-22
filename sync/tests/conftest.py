import testing.postgresql
import pytest

postgresql = testing.postgresql.Postgresql()


@pytest.fixture(scope="session")
def session_setup(request):
    def fin():
        postgresql.stop()

    request.addfinalizer(fin)
