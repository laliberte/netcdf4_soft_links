#External:
import netCDF4
import time
import os
from socket import error as SocketError
import errno
import warnings
import requests
import requests_cache
import datetime

#Internal:
import safe_handling
import sessions

class Dataset:
    def __init__(self,url_name,remote_data_node='',timeout=120,cache=None,expire_after=datetime.timedelta(hours=1),session=None):
        self.url_name=url_name
        self.timeout=timeout
        self.cache=cache
        self.expire_after=expire_after
        self.passed_session=session
        return

    def __enter__(self):
        if (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            self.session=self.passed_session
        else:
            self.session=sessions.create_single_session(cache=self.cache,expire_after=self.expire_after)

        #Disable cache for streaming get:
        if isinstance(self.session,requests_cache.core.CachedSession):
            with self.session.cache_disabled():
                return self._initiate_query()
        else:
            return self._initiate_query()

    def _initiate_query(self):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message='Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')
            headers = {'connection': 'close'}
            self.response = self.session.get(self.url_name, 
                        cert=(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY']),
                        verify=False,
                        headers=headers,
                        allow_redirects=True,
                        timeout=self.timeout,
                        stream=True)

        if self.response.ok:
            try:
                #content size must be larger than 0.
                #when content-length key exists
                content_size=int(self.response.headers['Content-Length'])
                if content_size==0:
                   raise RemoteEmptyError('URL {0} is empty. It will not be considered'.format(self.url_name))
            except KeyError:
                #Assume success:
                pass
        return self

    def __exit__(self,type,value,traceback):
        self.response.close()
        if not (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            self.session.close()
        return

class RemoteEmptyError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
