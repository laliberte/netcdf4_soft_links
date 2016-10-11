#External:
import netCDF4
import h5netcdf.legacyapi as netCDF4_h5
import netcdf4_pydap

import time
import os
import datetime
from socket import error as SocketError
import requests
from urllib2 import HTTPError
import copy

#Internal:
from . import safe_handling
from .. import netcdf_utils

class queryable_netCDF:
    def __init__(self,file_name,
                 semaphores=dict(),
                 time_var='time',
                 remote_data_node='',
                 cache=None,
                 timeout=120,
                 expire_after=datetime.timedelta(hours=1),
                 session=None,
                 username=None,
                 password=None,
                 authentication_url=None,
                 use_certificates=False):
        self.file_name=file_name
        self.semaphores=semaphores
        self.time_var=time_var

        if (remote_data_node in  self.semaphores.keys()):
            self.semaphore=semaphores[remote_data_node]
            self.handle_safely=True
        else:
            self.semaphore=safe_handling.dummy_semaphore()
            self.handle_safely=False

        self.cache=cache
        self.timeout=timeout
        self.expire_after=expire_after
        self.session=session
        self.authentication_url = authentication_url
        self.username=username
        self.password=password
        self.use_certificates=use_certificates

        if len(self.file_name)>4 and self.file_name[:4]=='http':
            self.use_pydap=True
            self.max_request=450
            #self.use_h5=False
        else:
            self.use_pydap=False
            self.max_request=2048
            try:
                with netCDF4_h5.Dataset(self.file_name,'r') as dataset:
                    pass
                self.use_h5=True
            except:
                self.use_h5=False
        return

    def __enter__(self):
        self.semaphore.acquire()
        return self

    def __exit__(self,type,value,traceback):
        if self.handle_safely:
            #Do not release semaphore right away if data is not local:
            time.sleep(0.01)
        self.semaphore.release()
        return

    def unsafe_handling(self,function_handle,*args,**kwargs):
        #Capture errors. Important to prevent curl errors from being printed:
        if self.use_pydap:
            with netcdf4_pydap.Dataset(self.file_name,cache=self.cache,
                                       timeout=self.timeout,
                                       expire_after=self.expire_after,
                                       session=self.session,
                                       authentication_url=self.authentication_url,
                                       username=self.username,
                                       password=self.password,
                                       use_certificates=self.use_certificates) as dataset:
                output=function_handle(dataset,*args,**kwargs)
        elif self.use_h5:
            with netCDF4_h5.Dataset(self.file_name,'r') as dataset:
                output=function_handle(dataset,*args,**kwargs)
        else:
            with netCDF4.Dataset(self.file_name,'r') as dataset:
                output=function_handle(dataset,*args,**kwargs)
        return output

    def safe_handling(self,function_handle, *args, **kwargs):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        if 'num_trials' in kwargs:
            num_trials = kwargs['num_trials']
            del kwargs['num_trials']
        else:
            num_trials = 5
        success=False
        timeout=copy.copy(self.timeout)
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    if self.use_pydap:
                        with netcdf4_pydap.Dataset(self.file_name,
                                                   cache=self.cache,
                                                   timeout=timeout,
                                                   expire_after=self.expire_after,
                                                   session=self.session,
                                                   authentication_url=self.authentication_url,
                                                   username=self.username,
                                                   password=self.password,
                                                   use_certificates=self.use_certificates) as dataset:
                            try:
                                output=function_handle(dataset,*args,**kwargs)
                            except EOFError as e:
                                #There is an issue with the remote file. Return default:
                                output=function_handle(dataset,*args,default=True,**kwargs)
                    elif self.use_h5:
                        with netCDF4_h5.Dataset(self.file_name,'r') as dataset:
                            output=function_handle(dataset,*args,**kwargs)
                    else:
                        with netCDF4.Dataset(self.file_name,'r') as dataset:
                            output=function_handle(dataset,*args,**kwargs)
                    success=True
                except RuntimeError:
                    time.sleep(3*(trial+1))
                    pass
                except requests.exceptions.ReadTimeout as e:
                    time.sleep(3*(trial+1))
                    #Increase timeout:
                    timeout+=self.timeout
                    pass
                except HTTPError as e:
                    time.sleep(3*(trial+1))
                    #Increase timeout:
                    timeout+=self.timeout
                    pass
                except requests.exceptions.ConnectionError as e:
                    time.sleep(3*(trial+1))
                    pass
                except SocketError as e:
                    #http://stackoverflow.com/questions/20568216/python-handling-socket-error-errno-104-connection-reset-by-peer
                    if e.errno != errno.ECONNRESET:
                        raise
                    time.sleep(3*(trial+1))
                    pass
        if not success:
            raise dodsError(error_statement)
        return output

    def check_if_opens(self,num_trials=5):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        return self.safe_handling(netcdf_utils.check_if_opens, num_trials=num_trials)

    def download(self,var,pointer_var,dimensions=dict(),unsort_dimensions=dict(),sort_table=[],time_var='time'):
        retrieved_data=self.safe_handling(
                         netcdf_utils.retrieve_container,var,
                                                        dimensions,
                                                        unsort_dimensions,
                                                        sort_table,self.max_request,
                                                        time_var=self.time_var,
                                                        file_name=self.file_name
                                        )
        return (retrieved_data, sort_table, pointer_var+[var])

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
