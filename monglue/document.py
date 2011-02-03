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
    def new(klass, database, document=None):
        if not document:
            document = {}
        _validate(klass, document)
        c = database[klass.__collection_name__]
        _id = c.insert(document)
        return klass(document)

    @classmethod
    def find(klass, database, spec=None):
        return database[klass.__collection_name__].find(spec, as_class=klass)

    @classmethod
    def find_one(klass, database, spec=None):
        return database[klass.__collection_name__].find_one(spec, as_class=klass)

    def set(self, database, document):
        self.update(document)
        _validate(self, self)
        return database[self.__collection_name__].update(
            {'_id': self['_id']}, {'$set': document})
