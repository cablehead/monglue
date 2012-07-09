Monglue
=======

Features
--------

Built in connection auto reconnect.

Connections default to safe mode.  It's easy to set common connection defaults.

Normalizes pymongo method names, so they aren't special cased for the given
instance type you are working with. This is useful in tests, as you can pass a
sub-collection for your tests to use, and they can transparently use the
sub-collection as though it were a database.  For example:

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

