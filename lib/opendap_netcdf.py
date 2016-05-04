#External:
import netCDF4
import time
import os

# Define a context manager to suppress stdout and stderr.
#http://stackoverflow.com/questions/11130156/suppress-stdout-stderr-print-from-python-functions
class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in 
    Python, i.e. will suppress all print, even if the print originates in a 
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).      

    '''
    def __init__(self):
        # Open a pair of null files
        self.null_fds =  [os.open(os.devnull,os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))
        #print(self.null_fds,self.save_fds)
        return

    def __enter__(self):

        # Assign the null pointers to stdout and stderr.
        for id in range(2):
            os.dup2(self.null_fds[id],id+1)
        return

    def __exit__(self, *_):
        for id in range(2):
            # Re-assign the real stdout/stderr back to (1) and (2)
            os.dup2(self.save_fds[id],id+1)
        return

    def close(self):
        for id in range(2):
            # Close the null files
            os.close(self.null_fds[id])
        #Close the duplicates:
        #Very important otherwise "too many files open"
        map(os.close,self.save_fds)
        return

class dummy_semaphore:
    def acquire(self):
        return 
    def release(self):
        return

class opendap_netCDF:
    def __init__(self,netcdf_file_name,semaphores=dict(),remote_data_node=''):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        if (remote_data_node in  self.semaphores.keys()):
            self.semaphore=semaphores[remote_data_node]
            self.handle_safely=True
        else:
            self.semaphore=dummy_semaphore()
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
            redirection=suppress_stdout_stderr()
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
        num_trials=5
        redirection=suppress_stdout_stderr()
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

    def check_if_opens(self):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        num_trials=5
        redirection=suppress_stdout_stderr()
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    #Capture errors. Important to prevent curl errors from being printed:
                    with redirection:
                        with netCDF4.Dataset(self.file_name) as dataset:
                            pass
                    success=True
                except:
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

        
