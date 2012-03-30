import logging
import pymongo


connection_defaults = {'safe': True}


def Connection(*a, **kw):
    for key in connection_defaults:
        kw.setdefault(key, connection_defaults[key])
    conn = autoreconnect(pymongo.Connection, *a, **kw)
    conn = AutoReconnectConnection(conn)
    return conn


# autoreconnect handling
def autoreconnect(f, *a, **kw):
    max_tries = 18
    num_tries = 1
    while True:
        try:
            return f(*a, **kw)

        except pymongo.errors.AutoReconnect, e:
            logging.warning(
                'MONGO-AUTORECONNECT attempt=%s %s' % (num_tries, e))
            num_tries += 1
            if num_tries > max_tries:
                raise
            time.sleep(0.2*num_tries)

        except pymongo.errors.OperationFailure, e:
            if 'duplicate key error' in str(e):
                raise
            logging.warning(
                'MONGO-AUTORECONNECT attempt=%s %s' % (num_tries, e))
            num_tries += 1
            if num_tries > max_tries:
                raise
            time.sleep(0.2*num_tries)


class AutoReconnectCallable(object):
    def __init__(self, method):
        self.method = method

    def __call__(self, *a, **kw):
        ret = autoreconnect(self.method, *a, **kw)
        if isinstance(ret, pymongo.cursor.Cursor):
            return AutoReconnectCursor(ret)
        return ret


class AutoReconnectCursor(object):
    def __init__(self, target):
        self.__target = target

    def __iter__(self):
        return self

    def next(self):
        return autoreconnect(self.__target.next)

    def __getattr__(self, name):
        ob = getattr(self.__target, name, None)
        if callable(ob):
            return AutoReconnectCallable(ob)
        return ob


class AutoReconnectConnection(object):
    def __init__(self, target):
        self.__target = target

    def names(self):
        if isinstance(self.__target, pymongo.connection.Connection):
            return self.__target.database_names()
        if isinstance(self.__target, pymongo.collection.Collection):
            return [
                x[len(self.__target.name+'.'):]
                    for x in self.__target.database.collection_names()
                        if x.startswith(self.__target.name+'.')]
        return self.__target.collection_names()

    def __getattr__(self, name):
        ob = getattr(self.__target, name, None)
        if ob == None or isinstance(ob, pymongo.collection.Collection):
            return self.__getitem__(name)
        if callable(ob):
            return AutoReconnectCallable(ob)
        return ob

    def __getitem__(self, name):
        return AutoReconnectConnection(self.__target.__getattr__(name))
