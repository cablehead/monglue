import unittest

from monglue.test.test_mongo import PyMongoStub
from monglue.document import Document


class User(Document):
    __collection_name__ = 'users'
    def truncate_name(self):
        return '%s %s.' % (self['first_name'], self['last_name'][0])


class DoumentTest(unittest.TestCase):
    def setUp(self):
        self._db = PyMongoStub()['foo']

    def test_new(self):
        u = User.new(
            self._db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        self.assertEqual(u.truncate_name(), 'Daniel H.')
        self.assertTrue('_id' in u)

    def test_find(self):
        User.new(self._db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        User.new(self._db, {'first_name': 'Andy', 'last_name': 'Gayton'})
        got = User.find(self._db)
        got.sort(lambda x,y: cmp(x['first_name'], y['first_name']))
        self.assertEqual(
            [u.truncate_name() for u in got],
            ['Andy G.', 'Daniel H.'])

    def test_set(self):
        u = User.new(self._db, {'first_name': 'Ted', 'last_name': 'Burns'})
        _id = u['_id']
        u.set(self._db, {'first_name': 'Ned'})
        self.assertEqual(
            u, {'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'})
        self.assertEqual(
            User.find(self._db),
            [{'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'}])
