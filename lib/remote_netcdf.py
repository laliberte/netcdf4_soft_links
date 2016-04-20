#External:
import netCDF4
import numpy as np
import time
import sys
import os

#Internal:
import netcdf_utils
import timeaxis_mod

#http://stackoverflow.com/questions/6796492/temporarily-redirect-stdout-stderr
class RedirectStdStreams(object):
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

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
    def __enter__(self):
        return 
    def __exit__(self, type, value, traceback):
        return

local_queryable_file_types=['local_file','soft_links_container']
remote_queryable_file_types=['OPENDAP']
#queryable_file_types=local_queryable_file_types+remote_queryable_file_types
queryable_file_types=['local_file','OPENDAP']
downloadable_file_types=['FTPServer','HTTPServer','GridFTP']

class remote_netCDF:
    def __init__(self,netcdf_file_name,file_type,semaphores,data_node=[],Xdata_node=[]):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        self.file_type=file_type
        self.remote_data_node=get_data_node(self.file_name, self.file_type)
        if (self.remote_data_node in  self.semaphores.keys()):
            self.semaphore=semaphores[self.remote_data_node]
        else:
            self.semaphore=dummy_semaphore()
        #Determine whether the remote data_node is local
        self.is_local=os.path.isdir(self.remote_data_node)
        self.Dataset=None
        self.data_node=data_node
        self.Xdata_node=Xdata_node
        return
    
    def close(self):
        #if isinstance(self.Dataset,netCDF4.Dataset):
        try:
            self.Dataset.close()
        except:
            pass
        #del self.Dataset
        self.Dataset=None
        return

    def open(self):
        self.Dataset=netCDF4.Dataset(self.file_name)
        return

    def safe_handling(self,function_handle,*args):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        num_trials=5
        redirection=suppress_stdout_stderr()
        with self.semaphore:
            success=False
            for trial in range(num_trials):
                if not success:
                    try:
                        #Capture errors. Important to prevent curl errors from being printed:
                        with redirection:
                            self.open()
                            output=function_handle(self.Dataset,*args)
                        success=True
                    except RuntimeError:
                        time.sleep(10*(trial+1))
                        pass
                    finally:
                        self.close()
            if not self.is_local:
                #Do not release semaphore right away if data is not local:
                time.sleep(1)
        redirection.close()
        if not success:
            raise dodsError(error_statement)
        return output, success

    def open_with_error(self):
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
                        self.open()
                    success=True
                except:
                    time.sleep(10*(trial+1))
                    #print('Could have had a DAP error')
                    pass
                finally:
                    self.close()
            if not self.is_local:
                #Do not release semaphore right away if data is not local:
                time.sleep(1)
        redirection.close()
        return success

    def is_available(self):
        if not self.file_type in queryable_file_types: 
            return True
        return self.is_queryable()

    def is_queryable(self):
        if not self.file_type in queryable_file_types: 
            return False
        success=self.open_with_error()
        return success

    def check_if_available_and_find_alternative(self,paths_list,file_type_list,checksum_list,acceptable_file_types):
        if ( not self.file_type in acceptable_file_types or not self.is_available()):
            checksum=checksum_list[list(paths_list).index(self.file_name)]
            for cs_id, cs in enumerate(checksum_list):
                if ( cs==checksum and 
                     paths_list[cs_id]!=self.file_name and
                     file_type_list[cs_id] in acceptable_file_types  and
                     is_level_name_included_and_not_excluded('data_node',self,get_data_node(paths_list[cs_id],file_type_list[cs_id]))
                     ):
                        remote_data=remote_netCDF(paths_list[cs_id],file_type_list[cs_id],self.semaphores)
                        if remote_data.is_available():
                            return paths_list[cs_id]
            return None
        else:
            return self.file_name

    def retrieve_dimension(self,dimension):
        (data, attributes) ,success=self.safe_handling(netcdf_utils.retrieve_dimension,dimension)
        if not success:
            data,attributes=np.array([]),dict()
        return data, attributes

    def retrieve_dimension_list(self,var):
        dimensions,success=self.safe_handling(netcdf_utils.retrieve_dimension_list,var)
        if not success:
            dimensions=tuple()
        return dimensions

    def variables_list_with_time_dim(self,output,time_dim):
        variables_list, success=self.safe_handling(netcdf_utils.variables_list_with_time_dim,output,time_dim)
        if not success:
            variables_list=[]
        return variables_list

    def retrieve_dimension_type(self):
        dimension_type, success=self.safe_handling(netcdf_utils.find_dimension_type)
        if not success:
            dimension_type=dict()
        return dimension_type
    
    def retrieve_variables(self,output,zlib=False):
        output, success=self.safe_handling(netcdf_utils.retrieve_variables_safe,output)
        return output

    def retrieve_variables_no_time(self,output,time_dim,zlib=False):
        output, success=self.safe_handling(netcdf_utils.retrieve_variables_no_time_safe,output,time_dim)
        return output

    def grab_indices(self,var,indices,unsort_indices):
        data,success=self.safe_handling(netcdf_utils.grab_indices,var,indices,unsort_indices)
        if not success:
            data=np.array([])
        return data

    def find_time_dim(self):
        time_dim,success=self.safe_handling(netcdf_utils.find_time_dim)
        if not success:
            time_dim='time'
        return time_dim

    def replicate_netcdf_file(self,output):
        output,success=self.safe_handling(netcdf_utils.replicate_netcdf_file_safe,output)
        return output

    def replicate_netcdf_var(self,output,var):
        output,success=self.safe_handling(netcdf_utils.replicate_netcdf_var_safe,output,var)
        return output

    def replicate_netcdf_other_var(self,output,var,time_dim):
        output,success=self.safe_handling(netcdf_utils.replicate_netcdf_other_var_safe,output,var,time_dim)
        return output

    def get_time(self,time_frequency=None,is_instant=False,calendar='standard'):
        if self.file_type in queryable_file_types:
            date_axis,success=self.safe_handling(netcdf_utils.get_time)
            if not success:
                date_axis=np.array([])
            return date_axis
        elif time_frequency!=None:
            start_date,end_date=dates_from_filename(self.file_name,calendar)
            units=self.get_time_units(calendar)
            start_id=0

            funits=timeaxis_mod.convert_time_units(units, time_frequency)
            end_id=timeaxis_mod.Date2num(end_date,funits,calendar)

            inc = timeaxis_mod.time_inc(time_frequency)
            length=max(end_id/inc-2,1.0)
            
            last_rebuild=start_date
            if last_rebuild == end_date:
                date_axis=rebuild_date_axis(0, length, is_instant, inc, funits,calendar=calendar)
                return date_axis

            while last_rebuild < end_date:
                date_axis=rebuild_date_axis(0, length, is_instant, inc, funits,calendar=calendar)
                last_rebuild=date_axis[-1]
                length+=1
            return date_axis
        else:
            raise StandardError('time_frequency not provided for non-queryable file type.')
            return date_axis

    def get_calendar(self):
        if self.file_type in queryable_file_types:
            calendar,success=self.safe_handling(netcdf_utils.netcdf_calendar)
            if not success:
                calendar='standard'
        else:
            calendar='standard'
        return calendar

    def get_time_units(self,calendar):
        #Get units from filename:
        start_date,end_date=dates_from_filename(self.file_name,calendar)
        if self.file_type in queryable_file_types:
            units, success=self.safe_handling(netcdf_utils.netcdf_time_units)
            if not success:
                units='days since '+str(start_date)
        else:
            units='days since '+str(start_date)
                
        return units

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def dates_from_filename(filename, calendar):
    """
    Returns datetime objetcs for start and end dates from the filename.

    :param str filename: The filename
    :param str calendar: The NetCDF calendar attribute

    :returns: ``datetime`` instances for start and end dates from the filename
    :rtype: *datetime.datetime*
    This code is adapted from cmip5-timeaxis.

    """
    dates = []
    for date in filename.split('.')[-2].split('_')[-1].split('-'):
        digits = timeaxis_mod.untroncated_timestamp(date)
        # Convert string digits to %Y-%m-%d %H:%M:%S format
        date_as_since = ''.join([''.join(triple) for triple in zip(digits[::2], digits[1::2], ['', '-', '-', ' ', ':', ':', ':'])])[:-1]
        # Use num2date to create netCDF4 datetime objects
        dates.append(netCDF4.num2date(0.0, units='days since ' + date_as_since, calendar=calendar))
    return dates

def rebuild_date_axis(start, length, instant, inc, units,calendar='standard'):
    """
    Rebuilds date axis from numerical time axis, depending on MIP frequency, calendar and instant status.

    :param float date: The numerical date to start (from ``netCDF4.date2num`` or :func:`Date2num`)
    :param int length: The time axis length (i.e., the timesteps number)
    :param boolean instant: The instant status (from :func:`is_instant_time_axis`)
    :param int inc: The time incrementation (from :func:`time_inc`)

    :returns: The corresponding theoretical date axis
    :rtype: *datetime array*

    """
    num_axis = np.arange(start=start, stop=start + length * inc, step=inc)
    if units.split(' ')[0] in ['years', 'months']:
        last = timeaxis_mod.Num2date(num_axis[-1], units=units, calendar=calendar)[0]
    else:
        last = timeaxis_mod.Num2date(num_axis[-1], units=units, calendar=calendar)
    if not instant and not inc in [3, 6]:  # To solve non-instant [36]hr files
        num_axis += 0.5 * inc
    date_axis = timeaxis_mod.Num2date(num_axis, units=units, calendar=calendar)
    return date_axis



#def ensure_zero(indices):
#    if indices.start > 0:
#        return [0,]+range(0,indices.stop)[indices]
#    else:
#        return indices

#def remove_zero_if_added(arr,indices,dim_id):
#    if indices.start > 0:
#        return np.take(arr,range(1,arr.shape[dim_id]),axis=dim_id)
#    else:
#        return arr

def get_data_node(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='OPENDAP':
        return '/'.join(path.split('/')[:3])
    elif file_type=='FTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:2])
        #return path.split('/')[0]
    elif file_type=='soft_links_container':
        return 'soft_links_container'
    else:
        return ''
        
def is_level_name_included_and_not_excluded(level_name,options,group):
    if level_name in dir(options):
        if isinstance(getattr(options,level_name),list):
            included=((getattr(options,level_name)==[]) or
                     (group in getattr(options,level_name)))
        else:
            included=((getattr(options,level_name)==None) or 
                       (getattr(options,level_name)==group)) 
    else:
        included=True

    if 'X'+level_name in dir(options):
        if isinstance(getattr(options,'X'+level_name),list):
            not_excluded=((getattr(options,'X'+level_name)==[]) or
                     (not group in getattr(options,'X'+level_name)))
        else:
            not_excluded=((getattr(options,'X'+level_name)==None) or 
                           (getattr(options,'X'+level_name)!=group)) 
    else:
        not_excluded=True
    return included and not_excluded
