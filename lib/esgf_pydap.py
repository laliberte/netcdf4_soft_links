"""
Based on pydap.util.http
Frederic Laliberte,2016
"""

import re
from urlparse import urlsplit, urlunsplit

import requests
import requests_cache
import warnings

import pydap.lib
import pydap.client
from pydap.model import BaseType, SequenceType
from pydap.exceptions import ServerError
from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.lib import walk, combine_slices, fix_slice, parse_qs, fix_shn, encode_atom

import os
import datetime
import numpy as np

from collections import OrderedDict

#Internal:
import esgf_pydap_proxy
import requests_sessions

class Dataset:
    def __init__(self,url,cache=None,expire_after=datetime.timedelta(hours=1),timeout=120,session=None):
        self.url=url
        self.cache=cache
        self.expire_after=expire_after
        self.timeout=timeout
        self.passed_session=session
        self.parent=self
        if (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            self.session=self.passed_session
        else:
            self.session=requests_sessions.create_single_session(cache=self.cache,expire_after=self.expire_after)

        for response in [self._ddx, self._ddsdas]:
            self.dataset = response()
            if self.dataset: break
        else:
            raise ClientError("Unable to open dataset.")

        # Remove any projections from the url, leaving selections.
        scheme, netloc, path, query, fragment = urlsplit(self.url)
        projection, selection = parse_qs(query)
        url = urlunsplit(
                (scheme, netloc, path, '&'.join(selection), fragment))

        # Set data to a Proxy object for BaseType and SequenceType. These
        # variables can then be sliced to retrieve the data on-the-fly.
        for var in walk(self.dataset, BaseType):
            var.data = esgf_pydap_proxy.ArrayProxy(var.id, url, var.shape,self._request)
        for var in walk(self.dataset, SequenceType):
            var.data = esgf_pydap_proxy.SequenceProxy(var.id, url,self._request)

        # Set server-side functions.
        self.dataset.functions = pydap.client.Functions(url)

        # Apply the corresponding slices.
        projection = fix_shn(projection, self.dataset)
        for var in projection:
            target = self.dataset
            while var:
                token, slice_ = var.pop(0)
                target = target[token]
                if slice_ and isinstance(target.data, VariableProxy):
                    shape = getattr(target, 'shape', (sys.maxint,))
                    target.data._slice = fix_slice(slice_, shape)

        self.dimensions=self._dimensions()
        self.variables=self._variables()

        self._is_open=True
        return

    def __enter__(self):
        return self

    def isopen(self):
        return self._is_open

    def groups(self):
        return dict()
    
    def ncattrs(self):
        return self.dataset.attributes['NC_GLOBAL'].keys()

    def getncattr(self,attr):
        return self.dataset.attributes['NC_GLOBAL'][attr]

    def _dimensions(self):
        if ('DODS_EXTRA' in self.dataset.attributes.keys() and
            'Unlimited_Dimension' in self.dataset.attributes['DODS_EXTRA']):
            unlimited_dims=[self.dataset.attributes['DODS_EXTRA']['Unlimited_Dimension'],]
        else:
            unlimited_dims=[]
        var_list=self.dataset.keys()
        var_id=np.argmax(map(len,[self.dataset[varname].dimensions for varname in var_list]))
        base_dimensions_list=self.dataset[var_list[var_id]].dimensions
        base_dimensions_lengths=self.dataset[var_list[var_id]].shape
        
        for varname in var_list:
            if not set(base_dimensions_list).issuperset(self.dataset[varname].dimensions):
                for dim_id,dim in enumerate(self.dataset[varname].dimensions):
                    if not dim in base_dimensions_list:
                        base_dimensions_list+=(dim,)
                        base_dimensions_lengths+=(self.dataset[varname].shape[dim_id],)
        dimensions_dict=OrderedDict()
        for dim,dim_length in zip( base_dimensions_list,base_dimensions_lengths):
            dimensions_dict[dim]=Dimension(dim,dim_length,(dim in unlimited_dims),self.dataset)
        return  dimensions_dict

    def _variables(self):
        return {var:Variable(self.dataset[var],var,self.dataset) for var in self.dataset.keys()}

    def get_variables_by_attributes(self,**kwargs):
        #From netcdf4-python
        vs = []

        has_value_flag  = False
        for vname in self.variables:
            var = self.variables[vname]
            for k, v in kwargs.items():
                if callable(v):
                    has_value_flag = v(getattr(var, k, None))
                    if has_value_flag is False:
                        break
                #elif hasattr(var, k) and getattr(var, k) == v:
                #Must use getncattr
                elif hasattr(var, k) and var.getncattr(k) == v:
                    has_value_flag = True
                else:
                    has_value_flag = False
                    break
            if has_value_flag is True:
                vs.append(self.variables[vname])
        return vs

    def set_auto_mask(self,flag):
        raise NotImplementedError('set_auto_mask is not implemented for pydap')
        return

    def set_auto_scale(self,flag):
        raise NotImplementedError('set_auto_scale is not implemented for pydap')
        return

    def filepath(self):
        return self.url

    def _request(self,url):
        """
        Open a given URL and return headers and body.
        This function retrieves data from a given URL, returning the headers
        and the response body. Authentication can be set by adding the
        username and password to the URL; this will be sent as clear text
        only if the server only supports Basic authentication.
        """
        scheme, netloc, path, query, fragment = urlsplit(url)
        url = urlunsplit((
                scheme, netloc, path, query, fragment
                )).rstrip('?&')

        headers = {
            'user-agent': pydap.lib.USER_AGENT,
            'connection': 'close'}
        with warnings.catch_warnings():
             warnings.filterwarnings('ignore', message='Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')
             resp =self.session.get(url, 
                        cert=(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY']),
                        verify=False,
                        headers=headers,
                        allow_redirects=True,
                        timeout=self.timeout)

        # When an error is returned, we parse the error message from the
        # server and return it in a ``ClientError`` exception.
        try:
            if resp.headers["content-description"] in ["dods_error", "dods-error"]:
                m = re.search('code = (?P<code>[^;]+);\s*message = "(?P<msg>.*)"',
                        resp.content, re.DOTALL | re.MULTILINE)
                msg = 'Server error %(code)s: "%(msg)s"' % m.groupdict()
                raise ServerError(msg)
        finally:
            resp.raise_for_status()

        return resp.headers, resp.content

    def __exit__(self,type,value,traceback):
        self.close()
        return

    def close(self):
        if not (isinstance(self.passed_session,requests.Session) or
            isinstance(self.passed_session,requests_cache.core.CachedSession)
            ):
            #Close the session
            self.session.close()
        return
        
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

_private_atts =\
['_grpid','_grp','_varid','groups','dimensions','variables','dtype','data_model','disk_format',
 '_nunlimdim','path','parent','ndim','mask','scale','cmptypes','vltypes','enumtypes','_isprimitive',
 'file_format','_isvlen','_isenum','_iscompound','_cmptype','_vltype','_enumtype','name',
 '__orthogoral_indexing__','keepweakref','_has_lsd']

class Variable:
    def __init__(self,var,name,dataset):
        self.var=var
        self.dimensions=self.var.dimensions
        if self.var.type.descriptor=='String':
            self.datatype='S1'
        else:
            self.datatype=self.var.type.typecode
        self.dtype=np.dtype(self.datatype)
        self.ndim=len(self.dimensions)
        self.shape=self.var.shape
        self.scale=True
        self.name=name
        self.size=np.prod(self.shape)
        self.dataset=dataset
        return

    def chunking(self):
        return 'contiguous'

    def filters(self):
        return None

    def get_var_chunk_cache(self):
        raise NotImplementedError('get_var_chunk_cache is not implemented')
        return

    #def __getattr__(self,name):
         # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        #if name.startswith('__') and name.endswith('__'):
        #    # if __dict__ requested, return a dict with netCDF attributes.
        #    if name == '__dict__':
        #        names = self.ncattrs()
        #        values = []
        #        for name in names:
        #            #values.append(_get_att(self._grp, self._varid, name))
        #            values.append(self.getncattr(name))
        #        return OrderedDict(zip(names,values))
        #    else:
        #        raise AttributeError
        #elif name in _private_atts:
        #    return self.__dict__[name]
        #else:
    #    return self.getncattr(name) 

    def ncattrs(self):
        return self.var.attributes.keys()

    def getncattr(self,attr):
        return self.var.attributes[attr]

    def getValue(self):
        return self.var[...]

    def group(self):
        return self.dataset

    #def __array__(self):
    #    return self[...]

    def __getitem__(self,getitem_tuple):
        try:
            return self.var.array.__getitem__(getitem_tuple)
        except (AttributeError, ServerError) as e:
            if ( 
                 isinstance(getitem_tuple,slice) and
                 getitem_tuple == phony_variable()[:]):
                #A single dimension ellipsis was requested. Use netCDF4 convention:
                return self.var[...]
            else:
                return self.var.__getitem__(getitem_tuple)

class phony_variable:
    #A phony variable to translate getitems:
    def __init__(self):
        pass

    def __getitem__(self,getitem_tuple):
        return getitem_tuple

class Dimension:
    def __init__(self,name,size,isunlimited,dataset):
        self.size=size
        self._isunlimited=isunlimited
        self.name=name
        self.dataset=dataset

    def __len__(self):
        return self.size

    def isunlimited(self):
        return self._isunlimited

    def group(self):
        return self.dataset



