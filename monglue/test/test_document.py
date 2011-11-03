import unittest

from monglue.test.test_mongo import PyMongoStub
from monglue.document import Document
from monglue.document import required
from monglue.document import optional
from monglue.document import ValidationError


class User(Document):
    __collection_name__ = 'users'
    def truncate_name(self):
        return '%s %s.' % (self['first_name'], self['last_name'][0])


class UserStrict(User):
    __collection_fields__ = {
        'first_name': required,
        'last_name': required,
        'age': optional,
    }


class DoumentTest(unittest.TestCase):
    def _get_database(self):
        return PyMongoStub()['foo']

    def test_new(self):
        db = self._get_database()
        u = User.new(db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        self.assertEqual(u.truncate_name(), 'Daniel H.')
        self.assertTrue('_id' in u)

    def test_find(self):
        db = self._get_database()
        User.new(db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        User.new(db, {'first_name': 'Andy', 'last_name': 'Gayton'})
        got = User.find(db)
        got.sort(lambda x,y: cmp(x['first_name'], y['first_name']))
        self.assertEqual(
            [u.truncate_name() for u in got], ['Andy G.', 'Daniel H.'])

    def test_find_one(self):
        db = self._get_database()
        User.new(db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        User.new(db, {'first_name': 'Andy', 'last_name': 'Gayton'})
        got = User.find_one(db, {'first_name': 'Daniel'})
        self.assertEqual(got.truncate_name(), 'Daniel H.')

    def test_set(self):
        db = self._get_database()
        u = UserStrict.new(db, {'first_name': 'Ted', 'last_name': 'Burns'})
        _id = u['_id']
        u.set(db, {'first_name': 'Ned'})
        self.assertEqual(
            u, {'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'})
        self.assertEqual(
            User.find(db),
            [{'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'}])

    def test_remove(self):
        db = self._get_database()
        u = UserStrict.new(db, {'first_name': 'Ted', 'last_name': 'Burns'})
        u.remove(db)
        got = User.find(db)
        self.assertEqual(got, [])

    def test_validation(self):
        db = self._get_database()
        u = UserStrict.new(
            db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})

    def test_validation_required(self):
        db = self._get_database()
        self.assertRaises(
            ValidationError,
            UserStrict.new, db, {'first_name': 'Daniel'})

    def test_validation_not_optional(self):
        db = self._get_database()
        self.assertRaises(
            ValidationError,
            UserStrict.new, db, {'address': '915 Hampshire St, San Francisco'})

    def test_validation_on_set(self):
        db = self._get_database()
        u = UserStrict.new(
            db, {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        self.assertRaises(
            ValidationError,
            u.set, db, {'address': '915 Hampshire St, San Francisco'})
