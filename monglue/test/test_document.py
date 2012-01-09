import unittest

import pymongo

from monglue.test.test_mongo import PyMongoStub
from monglue.document import Document
from monglue.document import Bind
from monglue.document import required
from monglue.document import optional
from monglue.document import ValidationError


class User(Document):
    __collection_name__ = 'users'
    def truncate_name(self):
        return '%s %s.' % (self.a['first_name'], self.a['last_name'][0])


class UserStrict(User):
    __collection_fields__ = {
        'first_name': required,
        'last_name': required,
        'age': optional,
    }
    __collection_indexes__ = [
        ([
            ('last_name', pymongo.DESCENDING),
            ('first_name', pymongo.ASCENDING),
        ], {'sparse': True})
    ]


class DoumentTest(unittest.TestCase):
    def _get_database(self):
        db = PyMongoStub()['foo']
        return Bind(db, User, UserStrict)

    def test_new(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        self.assertEqual(u.truncate_name(), 'Daniel H.')
        self.assertTrue('_id' in u.a)

    def test_find(self):
        db = self._get_database()
        db.User.new({'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        db.User.new({'first_name': 'Andy', 'last_name': 'Gayton'})
        got = db.User.find()
        got.sort(lambda x,y: cmp(x.a['first_name'], y.a['first_name']))
        self.assertEqual(
            [u.truncate_name() for u in got], ['Andy G.', 'Daniel H.'])

    def test_find_one(self):
        db = self._get_database()
        db.User.new({'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        db.User.new({'first_name': 'Andy', 'last_name': 'Gayton'})
        got = db.User.find_one({'first_name': 'Daniel'})
        self.assertEqual(got.truncate_name(), 'Daniel H.')

    def test_remove(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Ted', 'last_name': 'Burns'})
        u.remove()
        got = db.User.find()
        self.assertEqual(got, [])

    def test_drop(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Ted', 'last_name': 'Burns'})
        db.User.drop()
        got = db.User.find()
        self.assertEqual(got, [])

    def test_drop_safety(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Ted', 'last_name': 'Burns'})
        self.assertRaises(AssertionError, u.drop)

    def test_set(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Ted', 'last_name': 'Burns'})
        _id = u.a['_id']
        u.set({'first_name': 'Ned'})
        self.assertEqual(
            u.a, {'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'})
        self.assertEqual(
            [x.a for x in db.User.find()],
            [{'_id': _id, 'first_name': 'Ned', 'last_name': 'Burns'}])

    def test_addToSet(self):
        db = self._get_database()
        u = db.User.new({'first_name': 'Ted', 'last_name': 'Burns'})
        _id = u.a['_id']

        u.addToSet({'permissions': 'read'})
        self.assertEqual(u.a, {
                '_id': _id,
                'first_name': 'Ted',
                'last_name': 'Burns',
                'permissions': ['read']})

        u.addToSet({'permissions': 'write'})
        self.assertEqual(u.a, {
                '_id': _id,
                'first_name': 'Ted',
                'last_name': 'Burns',
                'permissions': ['read', 'write']})

        self.assertEqual(
            [x.a for x in db.User.find()], [{
                '_id': _id,
                'first_name': 'Ted',
                'last_name': 'Burns',
                'permissions': ['read', 'write']}])

    def test_validation(self):
        db = self._get_database()
        u = db.UserStrict.new(
            {'first_name': 'Daniel', 'last_name': 'Hengeveld'})

    def test_validation_required(self):
        db = self._get_database()
        self.assertRaises(
            ValidationError,
            db.UserStrict.new, {'first_name': 'Daniel'})

    def test_validation_not_optional(self):
        db = self._get_database()
        self.assertRaises(
            ValidationError,
            db.UserStrict.new, {'address': '915 Hampshire St, San Francisco'})

    def test_validation_on_set(self):
        db = self._get_database()
        u = db.UserStrict.new(
            {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        self.assertRaises(
            ValidationError,
            u.set, {'address': '915 Hampshire St, San Francisco'})

    def test_indexes(self):
        db = self._get_database()
        u = db.UserStrict.new(
            {'first_name': 'Daniel', 'last_name': 'Hengeveld'})
        index = u.index_information()
        self.assertEqual(
            index['last_name_-1_first_name_1'],
            {
                'key': [('last_name', -1), ('first_name', 1)],
                'sparse': True,
                'v': 1})
