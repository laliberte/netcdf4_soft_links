"""
Based on pydap.util.http
Frederic Laliberte,2016
"""

import re
from urlparse import urlsplit, urlunsplit
import logging

import requests
import requests_cache
import warnings

import pydap.lib
import pydap.client
import pydap.proxy
from pydap.exceptions import ServerError
from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
import os
from datetime import timedelta

log = logging.getLogger('esgf.security')

class Dataset:
    def __init__(self,url):
        if pydap.lib.CACHE:
            requests_cache.install_cache(pydap.lib.CACHE+'/esgf_cache',expire_after=timedelta(hours=1))
        self.url=url

    def __enter__(self):
        self.session=requests.Session()

        for response in [self._ddx, self._ddsdas]:
            dataset = response()
            if dataset: break
        else:
            raise ClientError("Unable to open dataset.")

        # Remove any projections from the url, leaving selections.
        scheme, netloc, path, query, fragment = urlsplit(self.url)
        projection, selection = parse_qs(query)
        url = urlunsplit(
                (scheme, netloc, path, '&'.join(selection), fragment))

        # Set data to a Proxy object for BaseType and SequenceType. These
        # variables can then be sliced to retrieve the data on-the-fly.
        for var in walk(dataset, BaseType):
            var.data = pydap.proxy.ArrayProxy(var.id, url, var.shape)
        for var in walk(dataset, SequenceType):
            var.data = pydap.proxy.SequenceProxy(var.id, url)

        # Set server-side functions.
        dataset.functions = pydap.client.Functions(url)

        # Apply the corresponding slices.
        projection = fix_shn(projection, dataset)
        for var in projection:
            target = dataset
            while var:
                token, slice_ = var.pop(0)
                target = target[token]
                if slice_ and isinstance(target.data, VariableProxy):
                    shape = getattr(target, 'shape', (sys.maxint,))
                    target.data._slice = fix_slice(slice_, shape)

        return dataset

    def _request(self,url):
        """
        Open a given URL and return headers and body.
        This function retrieves data from a given URL, returning the headers
        and the response body. Authentication can be set by adding the
        username and password to the URL; this will be sent as clear text
        only if the server only supports Basic authentication.
        """
        #h = httplib2.Http(cache=pydap.lib.CACHE,
        #        timeout=pydap.lib.TIMEOUT,
        #        proxy_info=pydap.lib.PROXY,
        #        disable_ssl_certificate_validation=True)
        #h.add_certificate(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY'],'')
        scheme, netloc, path, query, fragment = urlsplit(url)
        url = urlunsplit((
                scheme, netloc, path, query, fragment
                )).rstrip('?&')

        log.info('Opening %s' % url)
        headers = {
            'user-agent': pydap.lib.USER_AGENT,
            'connection': 'close'}
        with warnings.catch_warnings():
             warnings.filterwarnings('ignore', message='Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')
             resp =self.session.get(url, cert=(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY']),verify=False,headers=headers)

        # When an error is returned, we parse the error message from the
        # server and return it in a ``ClientError`` exception.
        if resp.headers["content-description"] in ["dods_error", "dods-error"]:
            m = re.search('code = (?P<code>[^;]+);\s*message = "(?P<msg>.*)"',
                    resp.content, re.DOTALL | re.MULTILINE)
            msg = 'Server error %(code)s: "%(msg)s"' % m.groupdict()
            raise ServerError(msg)

        return resp.headers, resp.content

    def _ddx(self):
        """
        Stub function for DDX.

        Still waiting for the DDX spec to write this.

        """
        pass


    def _ddsdas(self):
        """
        Build the dataset from the DDS+DAS responses.

        This function builds the dataset object from the DDS and DAS
        responses, adding Proxy objects to the variables.

        """
        scheme, netloc, path, query, fragment = urlsplit(self.url)
        ddsurl = urlunsplit(
                (scheme, netloc, path + '.dds', query, fragment))
        dasurl = urlunsplit(
                (scheme, netloc, path + '.das', query, fragment))

        respdds, dds = self._request(ddsurl)
        respdas, das = self._request(dasurl)

        # Build the dataset structure and attributes.
        dataset = DDSParser(dds).parse()
        dataset = DASParser(das, dataset).parse()
        return dataset

    def __exit__(self,type,value,traceback):
        #Close the session
        self.session.close()
        return
        
