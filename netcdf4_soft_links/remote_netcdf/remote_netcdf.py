#External:
import netCDF4
import numpy as np
import time
import sys
import os
import datetime
import netcdf4_pydap.esgf as esgf

#Internal:
from . import timeaxis_mod, queryable_netcdf, http_netcdf
from .. import netcdf_utils

local_queryable_file_types=['local_file','soft_links_container']
remote_queryable_file_types=['OPENDAP']
queryable_file_types=local_queryable_file_types+remote_queryable_file_types
downloadable_file_types=['FTPServer','HTTPServer','GridFTP']

class remote_netCDF:
    def __init__(self,filename,file_type,
                 semaphores=dict(),
                 session=None,
                 data_node=[],
                 Xdata_node=[],
                 time_var='time',
                 username=None,
                 password=None,
                 openid=None,
                 use_certificates=False,
                 cache=None,
                 timeout=120,
                 expire_after=datetime.timedelta(hours=1)):
        self.filename=filename
        self.file_type=file_type
        self.remote_data_node=get_data_node(self.filename,self.file_type)
        self.semaphores=semaphores

        self.data_node=data_node
        self.Xdata_node=Xdata_node

        self.cache=cache
        self.timeout=timeout
        self.time_var=time_var
        self.expire_after=expire_after
        self.session=session
        self.authentication_url=esgf.authentication_url(openid)
        self.openid=openid
        self.username=username
        self.password=password
        self.use_certificates=use_certificates
        return
    
    def is_available(self,num_trials=5):
        #Do not cache these responses. Must check EVERY time for robustness:
        if not self.file_type in queryable_file_types: 
            with http_netcdf.http_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=self.remote_data_node,
                                                timeout=self.timeout,
                                                session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.check_if_opens(num_trials=num_trials)
        elif not self.file_type in ['soft_links_container']:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=self.remote_data_node,
                                                timeout=self.timeout,
                                                session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.check_if_opens(num_trials=num_trials)
        else:
            #Any other case, assume true:
            return True

    def download(self,var,pointer_var,download_kwargs=dict()):
        if self.file_type in queryable_file_types:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                   semaphores=self.semaphores,
                                                   time_var=self.time_var,
                                                   remote_data_node=get_data_node(self.filename,self.file_type),
                                                   cache=self.cache,timeout=self.timeout,
                                                   expire_after=self.expire_after,session=self.session,
                                                   authentication_url=self.authentication_url,
                                                   username=self.username,
                                                   password=self.password,
                                                   use_certificates=self.use_certificates) as remote_data:
                return remote_data.download(var,pointer_var,**download_kwargs)
        elif self.file_type == 'HTTPServer':
            with http_netcdf.http_netCDF(self.filename,
                                         semaphores=self.semaphores,
                                         remote_data_node=get_data_node(self.filename,self.file_type),
                                         cache=self.cache,timeout=self.timeout,
                                         expire_after=self.expire_after,session=self.session,
                                         authentication_url=self.authentication_url,
                                         username=self.username,
                                         password=self.password,
                                         use_certificates=self.use_certificates) as remote_data:
                return remote_data.download(var,pointer_var,**download_kwargs)
        elif self.file_type == 'FTPServer':
            with ftp_netcdf.ftp_netCDF(self.filename,
                                       semaphores=self.semaphores,
                                       remote_data_node=get_data_node(self.filename,self.file_type),
                                       cache=self.cache,timeout=self.timeout,
                                       expire_after=self.expire_after,session=self.session) as remote_data:
                return remote_data.download(var,pointer_var,**download_kwargs)

    def check_if_available_and_find_alternative(self,paths_list,
                                                    file_type_list,
                                                    checksum_list,
                                                    acceptable_file_types,num_trials=5):
        if ( not self.file_type in acceptable_file_types or
             not self.is_available(num_trials=num_trials)):
            checksum=checksum_list[list(paths_list).index(self.filename)]
            for cs_id, cs in enumerate(checksum_list):
                if ( cs==checksum and 
                     paths_list[cs_id]!=self.filename and
                     file_type_list[cs_id] in acceptable_file_types  and
                     is_level_name_included_and_not_excluded('data_node',self,get_data_node(paths_list[cs_id],file_type_list[cs_id]))
                     ):
                        remote_data=remote_netCDF(paths_list[cs_id],
                                                  file_type_list[cs_id],
                                                  self.semaphores,
                                                  cache=self.cache,
                                                  timeout=self.timeout,
                                                  expire_after=self.expire_after,
                                                  session=self.session,
                                                  openid=self.openid,
                                                  username=self.username,
                                                  password=self.password,
                                                  use_certificates=self.use_certificates)
                        if remote_data.is_available(num_trials=num_trials):
                            return paths_list[cs_id]
            return None
        else:
            return self.filename

    def safe_handling(self,function_handle,*args,**kwargs):
        if self.file_type in queryable_file_types:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=get_data_node(self.filename,self.file_type),
                                                cache=self.cache,timeout=self.timeout,
                                                expire_after=self.expire_after,session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.safe_handling(function_handle,*args,**kwargs)
        else:
            kwargs['default']=True
            return function_handle(None,*args,**kwargs)

    def get_time(self,time_frequency=None,is_instant=False,time_var='time',calendar='standard'):
        if self.file_type in queryable_file_types:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=get_data_node(self.filename,self.file_type),
                                                cache=self.cache,
                                                timeout=self.timeout,
                                                expire_after=self.expire_after,
                                                session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.safe_handling(netcdf_utils.get_time,time_var=time_var)
        elif time_frequency!=None:
            start_date, end_date = dates_from_filename(self.filename,calendar)
            units=self.get_time_units(calendar)
            start_id=0

            funits=timeaxis_mod.convert_time_units(units, time_frequency)
            end_id=timeaxis_mod.Date2num(end_date,funits,calendar)

            inc = timeaxis_mod.time_inc(time_frequency)
            length = max(end_id/inc-2,1.0)
            
            last_rebuild = start_date
            #use date2num to safely convert dates. Otherwise it might fail for dates <1900:
            if (netCDF4.date2num(last_rebuild, units, calendar=calendar) == 
                netCDF4.date2num(end_date, units, calendar=calendar)):
                date_axis = rebuild_date_axis(0, length, is_instant, inc, funits, calendar=calendar)
                return date_axis

            #use date2num to safely convert dates. Otherwise it might fail for dates <1900:
            while (netCDF4.date2num(last_rebuild, units, calendar=calendar) <
                   netCDF4.date2num(end_date, units, calendar=calendar)):
                date_axis = rebuild_date_axis(0, length, is_instant, inc, funits, calendar=calendar)
                last_rebuild = date_axis[-1]
                length += 1
            return date_axis
        else:
            raise StandardError('time_frequency not provided for non-queryable file type.')
            return date_axis

    def get_calendar(self,time_var='time'):
        if self.file_type in queryable_file_types:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=get_data_node(self.filename,self.file_type),
                                                cache=self.cache,
                                                timeout=self.timeout,
                                                expire_after=self.expire_after,
                                                session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.safe_handling(netcdf_utils.netcdf_calendar,time_var=time_var)
        else:
            calendar='standard'
        return calendar

    def get_time_units(self,calendar,time_var='time'):
        #Get units from filename:
        if self.file_type in queryable_file_types:
            with queryable_netcdf.queryable_netCDF(self.filename,
                                                semaphores=self.semaphores,
                                                remote_data_node=get_data_node(self.filename,self.file_type),
                                                cache=self.cache,
                                                timeout=self.timeout,
                                                expire_after=self.expire_after,
                                                session=self.session,
                                                authentication_url=self.authentication_url,
                                                username=self.username,
                                                password=self.password,
                                                use_certificates=self.use_certificates) as remote_data:
                return remote_data.safe_handling(netcdf_utils.netcdf_time_units,time_var=time_var)
        else:
            start_date,end_date=dates_from_filename(self.filename,calendar)
            units='days since '+str(start_date)
        return units

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

def rebuild_date_axis(start, length, instant, inc, units, calendar='standard'):
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
