Monglue
=======

Monglue is a set of utilities for working with MongoDB, featuring a simple
wrapper for dict that takes care of common store operations and saves you from
exercising the low-level database driver in your tests. It is entirely a work
in progress, subject to change at any time, and implemented in reaction to
real-world issues encountered while building other projects rather than fully
designed up front.


Features
--------

Built in connection auto reconnect.

Connections default to safe mode.  It's easy to set common connection defaults.

Normalizing pymongo method names, so they aren't special cased for the given
instance type you are working with. This is useful in tests, as you can pass a
sub-collection for your tests to use, and it can treat the sub-collection as
though it were a database.  For example:

    >> conn = monglue.Connection()
    >> conn.names()
    >> ['db1', 'db2']

    >> db = conn.db1
    >> db.names()
    >> ['col1', 'col2.users', 'col2.groups']

    >> db['col2'].names()
    >> ['users', 'groups']

    >> db['col2']['users'].drop()
    >> db.drop()

