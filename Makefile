devenv:
	tox -e devenv

test:
	tox

devserver:
	. devenv/bin/activate && gunicorn -t 1000 sync.http.server:api --reload

docs:
	. devenv/bin/activate && sphinx-apidoc -f -o docs/source/ sync
