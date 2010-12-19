import fudge
import nose
from nose.tools import eq_ as eq
from nose.tools import assert_raises
from pymongo.objectid import ObjectId

from monglue.document import Document, Required, Optional, ValidationError

def test_init():
    # At some point I will want to assert that class attributes from the
    # document definition aren't present after init. For now I'll just hope no
    # one will choose field names from ['clear', 'copy', 'fromkeys', 'get',
    # 'has_key', 'items', 'iteritems', 'iterkeys', 'itervalues', 'keys', 'pop',
    # 'popitem', 'setdefault', 'update', 'values']
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
    class MyDoc(Document):
        opt_field = Optional()

    fake_coll = fudge.Fake('coll').expects('save').with_args(
        {'other_field': 'this is not happiness'},
        ).returns(ObjectId())
    doc = MyDoc({'other_field':'this is not happiness'}, collection=fake_coll)
    doc.save()

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_strict():
    class MyDoc(Document):
        __strict__ = True
        opt_field = Optional()
        req_field = Required()

    mydoc = MyDoc(collection='not called')
    mydoc['req_field'] = 'foo'
    mydoc['extra_field'] = 'bar'
    assert_raises(
        ValidationError,
        mydoc.save,
        )

def test_collection_class_attribute():
    default_coll = fudge.Fake('coll').expects('save').with_args(
        {'foo':'los angeles', 'bar':'portland'},
        ).returns(ObjectId())
    class MyDoc(Document):
        __collection__ = default_coll

    mydoc = MyDoc(
        {'foo':'los angeles', 'bar':'portland'},
        )
    mydoc.save()

def test_collection_passed_at_init():
    init_passed_coll = fudge.Fake('coll').expects('save').with_args(
        {'foo':'los angeles', 'bar':'portland'},
        ).returns(ObjectId())
    default_coll = 'Not called!'
    class MyDoc(Document):
        __collection__ = default_coll

    mydoc = MyDoc(
        {'foo':'los angeles', 'bar':'portland'},
        collection=init_passed_coll,
        )
    mydoc.save()

def test_collection_passed_at_save():
    default_coll = 'Not called!'
    init_passed_coll = 'Not called!'
    save_passed_coll = fudge.Fake('coll').expects('save').with_args(
        {'foo':'los angeles', 'bar':'portland'},
        ).returns(ObjectId())

    class MyDoc(Document):
        __collection__ = default_coll

    mydoc = MyDoc(
        {'foo':'los angeles', 'bar':'portland'},
        collection=init_passed_coll,
        )

    mydoc.save(collection=save_passed_coll)

