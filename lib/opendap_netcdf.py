#External:
import netCDF4
import time
import os

#Internal:
import safe_handling

class opendap_netCDF:
    def __init__(self,netcdf_file_name,semaphores=dict(),remote_data_node=''):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        if (remote_data_node in  self.semaphores.keys()):
            self.semaphore=semaphores[remote_data_node]
            self.handle_safely=True
        else:
            self.semaphore=safe_handling.dummy_semaphore()
            self.handle_safely=False
        return

    def __enter__(self):
        self.semaphore.acquire()
        return self

    def __exit__(self,type,value,traceback):
        if self.handle_safely:
            #Do not release semaphore right away if data is not local:
            time.sleep(0.1)
        self.semaphore.release()
        return
    def unsafe_handling(self,function_handle,*args):
        try:
            #Capture errors. Important to prevent curl errors from being printed:
            redirection=safe_handling.suppress_stdout_stderr()
            with redirection:
                with netCDF4.Dataset(self.file_name) as dataset:
                    output=function_handle(dataset,*args)
        finally:
            redirection.close()
        return output

    def safe_handling(self,function_handle,*args):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        num_trials=3
        redirection=safe_handling.suppress_stdout_stderr()
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    with redirection:
                        with netCDF4.Dataset(self.file_name) as dataset:
                            output=function_handle(dataset,*args)
                    success=True
                except RuntimeError:
                    time.sleep(10*(trial+1))
                    pass
        redirection.close()
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
        redirection=safe_handling.suppress_stdout_stderr()
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    with redirection:
                        with netCDF4.Dataset(self.file_name) as dataset:
                            pass
                    success=True
                except RuntimeError:
                    time.sleep(10*(trial+1))
                    #print('Could have had a DAP error')
                    pass
        redirection.close()
        return success

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

        
