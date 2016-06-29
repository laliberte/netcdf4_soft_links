#External:
import netCDF4
import time
import os
import datetime
from socket import error as SocketError
import requests
import copy

#Internal:
import safe_handling
import esgf_pydap
import netcdf_utils

class queryable_netCDF:
    def __init__(self,file_name,semaphores=dict(),time_var='time',remote_data_node='',cache=None,timeout=120,expire_after=datetime.timedelta(hours=1),session=None):
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
        if len(self.file_name)>4 and self.file_name[:4]=='http':
            self.use_pydap=True
            self.max_request=450
        else:
            self.use_pydap=False
            self.max_request=2048
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
        try:
            #Capture errors. Important to prevent curl errors from being printed:
            redirection=safe_handling.suppress_stdout_stderr()
            if self.use_pydap:
                with esgf_pydap.Dataset(self.file_name,cache=self.cache,
                                        timeout=self.timeout,
                                        expire_after=self.expire_after,
                                        session=self.session) as dataset:
                    output=function_handle(dataset,*args,**kwargs)
            else:
                with redirection:
                    with netCDF4.Dataset(self.file_name) as dataset:
                        output=function_handle(dataset,*args,**kwargs)
        finally:
            redirection.close()
        return output

    def safe_handling(self,function_handle,*args,**kwargs):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        num_trials=5
        success=False
        timeout=copy.copy(self.timeout)
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    if self.use_pydap:
                        with esgf_pydap.Dataset(self.file_name,
                                                cache=self.cache,
                                                timeout=timeout,
                                                expire_after=self.expire_after,
                                                session=self.session) as dataset:
                            output=function_handle(dataset,*args,**kwargs)
                    else:
                        try:
                            redirection=safe_handling.suppress_stdout_stderr()
                            with redirection:
                                with netCDF4.Dataset(self.file_name) as dataset:
                                    output=function_handle(dataset,*args,**kwargs)
                        finally:
                            redirection.close()
                    success=True
                except RuntimeError:
                    time.sleep(3*(trial+1))
                    pass
                except requests.exceptions.ReadTimeout as e:
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
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    if self.use_pydap:
                        with esgf_pydap.Dataset(self.file_name,
                                                    cache=self.cache,
                                                    timeout=self.timeout,
                                                    expire_after=self.expire_after,
                                                    session=self.session) as dataset:
                            pass
                    else:
                        try:
                            redirection=safe_handling.suppress_stdout_stderr()
                            with redirection:
                                with netCDF4.Dataset(self.file_name) as dataset:
                                    pass
                        finally:
                            redirection.close()
                    success=True
                except RuntimeError:
                    time.sleep(3*(trial+1))
                    #print('Could have had a DAP error')
                    pass
                except requests.exceptions.ReadTimeout as e:
                    time.sleep(3*(trial+1))
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
        return success

    def download(self,var,pointer_var,dimensions=dict(),unsort_dimensions=dict(),sort_table=[],time_var='time'):
        retrieved_data=self.safe_handling(
                         netcdf_utils.retrieve_container,var,
                                                        dimensions,
                                                        unsort_dimensions,
                                                        sort_table,self.max_request,
                                                        time_var=self.time_var
                                        )
        return (retrieved_data, sort_table, pointer_var+[var])

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
