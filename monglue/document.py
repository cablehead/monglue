class DocumentError(Exception):
    """Generic Monglue Document exception"""

class ValidationError(DocumentError):
    """Document vaidation failed"""

dict_fields = dir(dict)

class Document(dict):
    """MongoDB document manager"""        
    def __init__(self, *a, **kw):
        self._collection = kw.pop('collection', None)
        self.__field_validators = {}
        killem = []
        for attr in dir(self.__class__):
            if isinstance(self.__getattribute__(attr), DocumentField):
                self.__field_validators[attr] = self.__getattribute__(attr)._validator
                # XXX how to delattr? keeps causing errors
        super(Document, self).__init__(*a, **kw)

    def save(self, collection=None):
        if collection is None:
            collection = self._collection

        if collection is None: # don't like these 2 identical ifs
            raise DocumentError('You must specify a collection for save')

        self._validate()

        _id = collection.save(self)
        old_id = self.get('_id', None)
        if old_id is not None:
            assert old_id == _id
        else:
            self['_id'] = _id

    def _validate(self):
        for key, validate in self.__field_validators.iteritems():
            if not validate(self, key):
                raise ValidationError('%s required' % key)

#     TODO?
#     def check_exists(self):
#         pass


class DocumentField(object):
    """Base class of document field"""

class Required(DocumentField):
    """Required field of a Document"""
    # todo add real validators
    def __init__(self):
        def _validate(document, field_name):
            return field_name in document
        self._validator = _validate

class Optional(DocumentField):
    """Required field of a Document"""
    def __init__(self):
        def _validate(document, field_name):
            return True
        self._validator = _validate
