# Sync

[![Build Status](https://travis-ci.org/jim8786453/sync.svg?branch=master)](https://travis-ci.org/jim8786453/sync)

[![Coverage Status](https://coveralls.io/repos/github/jim8786453/sync/badge.svg?branch=master)](https://coveralls.io/github/jim8786453/sync?branch=master)

[![Documentation Status](https://readthedocs.org/projects/py-sync/badge/?version=latest)](http://py-sync.readthedocs.io/en/latest/?badge=latest)

Designed to simplify the process of keeping data sychronised between multiple clients. It provides an API to perform create, update or delete operations that will be synchronised to other clients, as well as keep up to date with changes they make.

For example, a poor but commonly used method of synchronising records of data between two systems is to read and write CSV files to an FTP site. This can be error prone and scales badly when more systems also need to synchronise the same data. Sync can be used to help manage this situatation and reduce the number of point-to-point connections that have to be maintained.

Currently Sync is work in progress and is not ready for production use out of the box.

The [documentation](http://py-sync.readthedocs.io/en/latest/?) is currently built and hosted on Read The Docs.

## Admin API
TODO

## Messaging API
TODO

## Database support
Sync can use PostgreSQL or MongoDB as its database. It also provides a mock (in-memory) storage backend used for development and testing. Some features will only be available if the underlying database supports them, for example PostgreSQL transactions.

## Running from source
TODO

## Project roadmap
TODO
