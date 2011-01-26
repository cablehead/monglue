import unittest
import pymongo
import bson
import uuid


class PyMongoBaseTest(object):
    """
    A base test suite which asserts an API provides sane pymongo behavior
    """
    def test_insert(self):
        c = self.get_collection()
        row = {'foo': 'bar', 'abc': 123}
        _id = c.insert(row)
        row['_id'] = _id

        got = list(c.find())
        self.assertEqual(got, [row])

    def test_update_set(self):
        c = self.get_collection()
        row = {'foo': 'bar', 'abc': 123}
        _id = c.insert(row)
        c.update({'_id': _id}, {'$set': {'foo': 'bar2'}})

        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar2', 'abc': 123}])

    def test_update_inc(self):
        c = self.get_collection()
        row = {'foo': 'bar', 'abc': 123}
        _id = c.insert(row)
        c.update({'_id': _id}, {'$inc': {'abc': 2}})

        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar', 'abc': 125}])

    def test_find(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = list(c.find({'age': 23}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['a', 'b'])

    def test_find_gt(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 20})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = list(c.find({'age': {'$gt': 21}}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['b', 'c'])

    def test_find_lt(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 20})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = list(c.find({'age': {'$lt': 25}}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['a', 'b'])

    def test_find_gte(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 20})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.insert({'name': 'd', 'age': 28})
        got = list(c.find({'age': {'$gte': 23}}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['b', 'c', 'd'])

    def test_find_lte(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 20})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.insert({'name': 'd', 'age': 28})
        got = list(c.find({'age': {'$lte': 26}}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['a', 'b', 'c'])

    def test_find_as_class(self):
        class Klass(dict):
            pass
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        got = list(c.find(as_class=Klass))
        self.assertTrue(isinstance(got[0], Klass))


class PyMongoIntegrationTest(unittest.TestCase, PyMongoBaseTest):
    """
    Test that the PyMongoBaseTest suite works against a real pymongo/MongoDB
    instance.

    Creating and removing Mongo databases is pretty slow, so this integration
    suite reuses a common database.
    """

    __dbname__ = '__monglue_integration_test_database__'

    def setUp(self):
        # XXX(andy) - the connection to use should be configurable from an
        # environment variable
        self.__con__ = pymongo.Connection()
        self.__db__ = self.__con__[self.__dbname__]

    def get_collection(self):
        return self.__db__[str(uuid.uuid4())]


class PyMongoCollectionStub(object):
    def __init__(self):
        self.__documents__ = []

    def insert(self, document):
        document['_id'] = bson.objectid.ObjectId()
        self.__documents__.append(document)
        return document['_id']

    def find(self, spec=None, as_class=None):
        #XXX(andy) - this should return a cursor
        if spec == None:
            spec = {}
        ret = [x for x in self.__documents__ if self._match_spec(spec, x)]
        if as_class:
            ret = [as_class(x) for x in ret]
        return ret

    def _match_spec(self, spec, row):
        matchers = {
            '$gt': lambda x,y: x > y,
            '$gte': lambda x,y: x >= y,
            '$lt': lambda x,y: x < y,
            '$lte': lambda x,y: x <= y,
        }
        for key in spec:
            target = spec[key]
            # for the moment assume the dict for advanced queries only have one
            # item
            if isinstance(target, dict) and target.keys()[0].startswith('$'):
                matcher = matchers[target.keys()[0]]
                target = target.values()[0]
            else:
                matcher = lambda x,y: x == y

            if not matcher(row[key], target):
                return False
        return True

    def _set(self, document, row):
        for key in document:
            row[key] = document[key]

    def _inc(self, document, row):
        for key in document:
            row[key] += document[key]

    def _update(self, document, row):
        for key in document:
            if key == '$set':
                self._set(document[key], row)
            if key == '$inc':
                self._inc(document[key], row)

    def update(self, spec, document):
        possible = [x for x in self.__documents__ if self._match_spec(spec, x)]
        if possible:
            self._update(document, possible[0])


class PyMongoDatabaseStub(object):
    def __init__(self):
        self.__collections__ = {}

    def __getitem__(self, name):
        if name not in self.__collections__:
            self.__collections__[name] = PyMongoCollectionStub()
        return self.__collections__[name]


class PyMongoStub(object):
    """
    Stub for the pymongo api for use in unit tests
    """
    def __init__(self):
        self.__databases__ = {}

    def __getitem__(self, name):
        if name not in self.__databases__:
            self.__databases__[name] = PyMongoDatabaseStub()
        return self.__databases__[name]

    
class PyMongoStubTest(unittest.TestCase, PyMongoBaseTest):
    """
    Assert PyMongoStub matches the expected behavior of pymongo
    """
    def get_collection(self):
        con = PyMongoStub()
        return con[str(uuid.uuid4())][str(uuid.uuid4())]

