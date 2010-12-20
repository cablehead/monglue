class Document(dict):
    @classmethod
    def new(klass, database, document):
        c = database[klass.__collection_name__]
        _id = c.insert(document)
        return klass(document)

    @classmethod
    def find(klass, database, spec=None):
        return database[klass.__collection_name__].find(spec, as_class=klass)
