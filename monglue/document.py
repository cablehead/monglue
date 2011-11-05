"""
Exceptions
"""
class DocumentError(Exception):
    """Generic Monglue Document exception"""


class ValidationError(DocumentError):
    """Document vaidation failed"""


"""
Validators
"""
def required(document, field_name):
    return field_name in document

def optional(document, field_name):
    return True

def _validate(klass, document):
    if hasattr(klass, '__collection_fields__'):
        validators = klass.__collection_fields__
        for key in [k for k in document if k != '_id']:
            if key not in validators:
                raise ValidationError('Unknown field: %s' % key)

        for key in validators:
            if not validators[key](document, key):
                raise ValidationError('Validation failed for: %s' % key)


class Document(dict):
    @classmethod
    def new(klass, document=None):
        if not document:
            document = {}
        _validate(klass, document)
        c = klass.__database__[klass.__collection_name__]
        _id = c.insert(document)
        return klass(document)

    @classmethod
    def find(klass, spec=None):
        return klass.__database__[klass.__collection_name__].find(
            spec, as_class=klass)

    @classmethod
    def find_one(klass, spec=None):
        return klass.__database__[klass.__collection_name__].find_one(
            spec, as_class=klass)

    def set(self, document):
        self.update(document)
        _validate(self, self)
        return self.__database__[self.__collection_name__].update(
            {'_id': self['_id']}, {'$set': document})

    def addToSet(self, document):
        # XXX - this could reuse the pymongo stub code
        for key in document:
            if key not in self:
                self[key] = []
            self[key] = list(set(self[key]) | set([document[key]]))
        _validate(self, self)
        return self.__database__[self.__collection_name__].update(
            {'_id': self['_id']}, {'$addToSet': document})

    def remove(self):
        return self.__database__[self.__collection_name__].remove(
            {'_id': self['_id']})

    def __getattr__(self, name):
        if name == 'a':
            return dict(self)
        raise AttributeError


class Bind(object):
    def __init__(self, database, *Klasses, **kw):
        self.store = kw.pop('store', self)
        for Klass in Klasses:
            setattr(self,
                Klass.__name__,
                type(Klass.__name__, (Klass,),
                    {'x': self.store, '__database__': database}))
