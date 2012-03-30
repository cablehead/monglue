import uuid
import unittest

from monglue import connection

class ConnectionTest(unittest.TestCase):
    def test_retry_wrapper(self):
        c = connection.Connection()
        self.assertEqual(type(c), connection.AutoReconnectConnection)
        self.assertEqual(type(c['__test__']), connection.AutoReconnectConnection)
        self.assertEqual(type(c['__test__'].foo), connection.AutoReconnectConnection)
        self.assertEqual(type(c['__test__'].foo.find), connection.AutoReconnectCallable)
        self.assertEqual(type(c['__test__'].foo.find({})), connection.AutoReconnectCursor)
        self.assertEqual(type(c['__test__'].foo.find({}).count), connection.AutoReconnectCallable)

    def test_AutoReconnectCursor(self):
        c = connection.Connection()
        bucket = c['__test__'][uuid.uuid1().hex]
        bucket.insert({'a': 1})
        bucket.insert({'a': 2})
        bucket.insert({'a': 3})
        cursor = bucket.find()
        self.assertEqual([x['a'] for x in cursor], [1,2,3])

    def test_names(self):
        dbname = uuid.uuid4().hex
        c = connection.Connection()

        c[dbname]['collection'].save({})
        c[dbname]['collection']['subcollection'].save({})

        # test databases
        self.assertTrue(dbname in c.names())
        self.assertTrue('local' in c.names())

        # test collections
        self.assertEqual(
            c[dbname].names(),
            ['collection', 'system.indexes', 'collection.subcollection'])

        # test sub collections
        self.assertEqual(
            c[dbname]['collection'].names(), ['subcollection'])
