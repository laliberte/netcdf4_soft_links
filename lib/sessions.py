#External:
import os
import datetime
from sqlite3 import DatabaseError
import requests
import requests_cache

def create_single_session(cache=None,expire_after=datetime.timedelta(hours=1)):
    if cache:
        try:
            session=requests_cache.core.CachedSession(cache,expire_after=expire_after)
        except DatabaseError:
            #Corrupted cache:
            os.remove(self.cache)
            session=requests_cache.core.CachedSession(cache,expire_after=expire_after)
    else:
        #Create a phony in-memory cached session and disable it:
        session=requests.Session()
    return session
