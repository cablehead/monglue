import fudge
import nose
from nose.tools import eq_ as eq
from nose.tools import assert_raises
from pymongo.objectid import ObjectId

from monglue.document import Document, Required, Optional, ValidationError

def test_init():
    raise nose.SkipTest()

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_required():
    class MyDoc(Document):
        req_field = Required()

    fake_coll = fudge.Fake('coll').expects('save').with_args(
        {'req_field': 'ho ho ho'},
        ).returns(ObjectId())
    good_doc = MyDoc({'req_field':'ho ho ho'}, collection=fake_coll)
    good_doc.save()

    bad_doc = MyDoc(collection='not called')
    assert_raises(
        ValidationError,
        bad_doc.save,
        )

def test_optional():
    raise nose.SkipTest()

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_strict():
    raise nose.SkipTest() # this needs a little thinking before i really test it
    class MyDoc(Document):
        opt_field = Optional()
        req_field = Required()

    fake_coll = fudge.Fake('coll').expects('save').with_args({'req_field': 'foo', 'extra_field': 'bar'})
    _id = ObjectId()
    fake_coll.returns(_id)

    mydoc = MyDoc(collection=fake_coll)
    mydoc['req_field'] = 'foo'
    mydoc['extra_field'] = 'bar'
    mydoc.save() # save
    eq(mydoc['_id'], _id)

def test_default_collection():
    raise nose.SkipTest()
