venv: venv/bin/activate
venv/bin/activate: requirements.txt
	test -d venv || virtualenv venv
	venv/bin/pip install -Ur requirements.txt
	venv/bin/pip install gunicorn
	touch venv/bin/activate

dev-build: venv
	venv/bin/python setup.py install

server: venv
	. venv/bin/activate && gunicorn -t 1000 sync.http.server:api --reload

test:
	tox

docs: venv
	. venv/bin/activate && sphinx-apidoc -f -o docs/source/ sync
