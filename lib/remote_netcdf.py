#External:
import netCDF4
import numpy as np
import time
import sys
import os

#External but related:
from netcdf4_safe_opendap import opendap_netcdf
from netcdf4_safe_opendap import netcdf_utils

#Internal:
import timeaxis_mod

local_queryable_file_types=['local_file','soft_links_container']
remote_queryable_file_types=['OPENDAP']
queryable_file_types=['local_file','OPENDAP']
downloadable_file_types=['FTPServer','HTTPServer','GridFTP']
class remote_netCDF:
    def __init__(self,netcdf_filename,file_type,semaphores=dict(),data_node=[],Xdata_node=[]):
        self.filename=netcdf_filename
        self.semaphores=sempahores
        self.data_node=data_node
        self.Xdata_node=Xdata_node
        return
    
    def is_available(self):
        if not self.file_type in queryable_file_types: 
            return True
        else:
            with opendap_netcdf.opendap_netCDF(self.filename,semaphores=self.sempahores) as remote_data:
                return remote_data.check_if_opens()

    def check_if_available_and_find_alternative(self,paths_list,file_type_list,checksum_list,acceptable_file_types):
        if ( not self.file_type in acceptable_file_types or not self.is_available()):
            checksum=checksum_list[list(paths_list).index(self.filename)]
            for cs_id, cs in enumerate(checksum_list):
                if ( cs==checksum and 
                     paths_list[cs_id]!=self.filename and
                     file_type_list[cs_id] in acceptable_file_types  and
                     is_level_name_included_and_not_excluded('data_node',self,opendap_netcdf.get_data_node(paths_list[cs_id],file_type_list[cs_id]))
                     ):
                        remote_data=remote_netCDF(paths_list[cs_id],file_type_list[cs_id],self.semaphores)
                        if remote_data.is_available():
                            return paths_list[cs_id]
            return None
        else:
            return self.filename

    def safe_handling(self,function_handle,*args,**kwargs):
        if self.file_type in queryable_file_types:
            with opendap_netcdf.opendap_netCDF(self.filename,semaphores=self.sempahores) as remote_data:
                return remote_data.safe_handling(function_handle,*args,**kwargs)
        else:
            kwargs['default']=True:
            return function_handle(*args,**kwargs)

    def get_time(self,time_frequency=None,is_instant=False,calendar='standard'):
        if self.file_type in queryable_file_types:
            with opendap_netcdf.opendap_netCDF(self.filename,semaphores=self.sempahores) as remote_data:
                return remote_data.safe_handling(netcdf_utils.get_time)
        elif time_frequency!=None:
            start_date,end_date=dates_from_filename(self.filename,calendar)
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
            with opendap_netcdf.opendap_netCDF(self.filename,semaphores=self.sempahores) as remote_data:
                return remote_data.safe_handling(netcdf_utils.netcdf_calendar)
        else:
            calendar='standard'
        return calendar

    def get_time_units(self,calendar):
        #Get units from filename:
        start_date,end_date=dates_from_filename(self.filename,calendar)
        if self.file_type in queryable_file_types:
            with opendap_netcdf.opendap_netCDF(self.filename,semaphores=self.sempahores) as remote_data:
                return remote_data.safe_handling(netcdf_utils.netcdf_time_units)
        else:
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
