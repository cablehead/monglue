import unittest
import pymongo
import bson
import uuid
import re


class Klass(dict):
    pass


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
        row = {'foo': 'bar'}
        _id = c.insert(row)

        c.update({'_id': _id}, {'$inc': {'abc': 2}})
        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar', 'abc': 2}])

        c.update({'_id': _id}, {'$inc': {'abc': 3}})
        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar', 'abc': 5}])

    def test_update_addToSet(self):
        c = self.get_collection()
        row = {'foo': 'bar'}
        _id = c.insert(row)

        c.update({'_id': _id}, {'$addToSet': {'myset': 2}})
        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar', 'myset': [2]}])

        c.update({'_id': _id}, {'$addToSet': {'myset': 'foo'}})
        got = list(c.find())

        c.update({'_id': _id}, {'$addToSet': {'myset': 2}})
        got = list(c.find())
        self.assertEqual(got, [{'_id': _id, 'foo': 'bar', 'myset': [2, 'foo']}])

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
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        got = list(c.find(as_class=Klass))
        self.assertTrue(isinstance(got[0], Klass))

    def test_find_regexp(self):
        c = self.get_collection()
        c.insert({'name': 'ToM', 'age': 23})
        c.insert({'name': 'tom', 'age': 23})
        c.insert({'name': 'Thomas', 'age': 23})
        c.insert({'name': 'jack', 'age': 26})
        pattern = re.compile('TOM', re.IGNORECASE)
        got = list(c.find({'name': pattern}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['ToM', 'tom'])

    def test_find_allow_empty_fields(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'c'})
        got = list(c.find({'age': 23}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['a'])

    def test_find_match_on_empty_field(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'c'})
        got = list(c.find({'age': None}))
        got.sort()
        self.assertEqual([x['name'] for x in got], ['c'])

    def test_find_one(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = c.find_one()
        self.assertEqual(got['name'], 'a')

    def test_find_one_by_id(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        _id = c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = c.find_one(_id)
        self.assertEqual(got['name'], 'b')

    def test_find_one_by_spec(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = c.find_one({'name': 'c'})
        self.assertEqual(got['name'], 'c')

    def test_find_one_as_class(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = c.find_one(as_class=Klass)
        self.assertTrue(isinstance(got, Klass))

    def test_find_one_not_found(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        got = c.find_one({'name': 'd'})
        self.assertEqual(got, None)

    def test_remove_all(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.remove()
        got = list(c.find())
        got.sort()
        self.assertEqual([x['name'] for x in got], [])

    def test_remove_by_id(self):
        c = self.get_collection()
        _id = c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.remove(_id)
        got = list(c.find())
        got.sort()
        self.assertEqual([x['name'] for x in got], ['b', 'c'])

    def test_remove_by_spec(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.remove({'age': 23})
        got = list(c.find())
        got.sort()
        self.assertEqual([x['name'] for x in got], ['c'])

    def test_drop(self):
        c = self.get_collection()
        c.insert({'name': 'a', 'age': 23})
        c.insert({'name': 'b', 'age': 23})
        c.insert({'name': 'c', 'age': 26})
        c.drop()
        got = list(c.find())
        got.sort()
        self.assertEqual([x['name'] for x in got], [])

    def test_indexes(self):
        c = self.get_collection()
        self.assertEqual(c.index_information(), {})

        name = c.ensure_index(
            [('name', pymongo.ASCENDING), ('age', pymongo.DESCENDING)])
        self.assertEqual(name, 'name_1_age_-1')
        want = {
            '_id_': {'key': [('_id', 1)], 'v': 1},
            'name_1_age_-1': {'key': [('name', 1), ('age', -1)], 'v': 1},}
        got = c.index_information()
        self.assertEqual(got, want)

        # stub should ensure unique-ness on write
        name = c.ensure_index(
            [('name', pymongo.ASCENDING), ('status', pymongo.DESCENDING)],
            unique=True)
        self.assertEqual(name, 'name_1_status_-1')
        want['name_1_status_-1'] = {
            'unique': True, 'key': [('name', 1), ('status', -1)], 'v': 1}
        got = c.index_information()
        self.assertEqual(got, want)


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
        self.__indexes__ = {}

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

    def find_one(self, spec_or_id=None, *a, **kw):
        if spec_or_id == None:
            spec_or_id = {}
        if isinstance(spec_or_id, bson.objectid.ObjectId):
            spec_or_id = {'_id': spec_or_id}
        found = self.find(spec_or_id, *a, **kw)
        if not len(found):
            return None
        return found[0]

    def remove(self, spec_or_object_id=None):
        if spec_or_object_id == None:
            self.__documents__ = []
            return

        if isinstance(spec_or_object_id, bson.objectid.ObjectId):
            self.__documents__ = [
                x for x in self.__documents__ if x['_id'] != spec_or_object_id]
            return

        self.__documents__ = [
            x for x in self.__documents__
                if not self._match_spec(spec_or_object_id, x)]

    def drop(self):
        self.__documents__ = []

    def index_information(self):
        return self.__indexes__

    def ensure_index(self, key, **kw):
        name = ''
        name = '_'.join(['%s_%s' % item for item in key])
        if not self.__indexes__:
            self.__indexes__['_id_'] = {'key': [('_id', 1)], 'v': 1}
        self.__indexes__[name] = {'key': key, 'v': 1}
        self.__indexes__[name].update(kw)
        return name

    def _match_spec(self, spec, row):
        def equals(source, target):
            if isinstance(target, re._pattern_type):
                return bool(re.search(target, source))
            return source == target

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
                matcher = equals

            if not matcher(row.get(key), target):
                return False
        return True

    def _set(self, document, row):
        for key in document:
            row[key] = document[key]

    def _inc(self, document, row):
        for key in document:
            if key not in row:
                row[key] = 0
            row[key] += document[key]

    def _addToSet(self, document, row):
        for key in document:
            if key not in row:
                row[key] = []
            row[key] = list(set(row[key]) | set([document[key]]))

    def _update(self, document, row):
        for key in document:
            if key == '$set':
                self._set(document[key], row)
            if key == '$inc':
                self._inc(document[key], row)
            if key == '$addToSet':
                self._addToSet(document[key], row)

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

