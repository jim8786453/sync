# Sync

Sync is designed to simplify the process of keeping data sychronised between multiple clients, known as nodes. It provides an API to perform create, update or delete operations that will be synchronised to the other nodes, as well as keep up with the changes made by those nodes.

For example, a poor but commonly used method of synchronising data between two systems is to read and write CSV files to an FTP site. This can be error prone and scales badly when more systems also need to synchronise. Sync can be used to help manage this situatation and reduce the number of point-to-point connections that have to be maintained.

[![Build Status](https://travis-ci.org/jim8786453/sync.svg?branch=master)](https://travis-ci.org/jim8786453/sync)

[![Documentation Status](https://readthedocs.org/projects/py-sync/badge/?version=latest)](http://py-sync.readthedocs.io/en/latest/?badge=latest)

The [documentation](http://py-sync.readthedocs.io/en/latest/?) is currently built and hosted on Read The Docs.
