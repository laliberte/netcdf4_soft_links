#External:
import numpy as np
import tempfile
import copy
import os
import datetime
from collections import OrderedDict

#Internal:
import remote_netcdf
import netcdf_utils

_queryable_file_types = ['OPENDAP', 'local_file', 'soft_link_container']
_unique_file_id_list = ['checksum_type', 'checksum', 'tracking_id']
_id_list = ['data_node', 'file_type', 'path']+_unique_file_id_list

class create_netCDF_pointers:
    def __init__(self,
                 file_type_list=[], data_node_list=[],
                 time_frequency=None,
                 time_var='time',
                 year=None,
                 month=None,
                 semaphores=dict(),
                 check_dimensions=False,
                 session=None, remote_netcdf_kwargs=dict()):

        self.file_type_list = file_type_list
        self.data_node_list = data_node_list

        self.semaphores = semaphores
        self.session = session
        self.remote_netcdf_kwargs = remote_netcdf_kwargs

        self.check_dimensions = check_dimensions

        self.time_frequency = time_frequency
        self.is_instant = False
        self.time_var = time_var

        self.month = month
        self.year = year

        self.paths_link = OrderedDict()
        self.indices_link = OrderedDict()
        return

    def __setitem__(self, key, val):
        paths_ordering = _order_paths_by_preference(
                                                    val,
                                                    self.file_type_list,
                                                    self.data_node_list,
                                                    check_dimensions=self.check_dimensions,
                                                    semaphores=self.semaphores,
                                                    time_var=self.time_var,
                                                    session=self.session,
                                                    remote_netcdf_kwargs=self.remote_netcdf_kwargs)
        if not isinstace(key,list):
            key = [key,]

        if self.time_frequency in ['fx','clim']:
            most_recent_version = 'v'+str(np.max([int(item['version'][1:]) for item in val]))
            self.usable_paths_list = [ item for item in val if item['version']==most_recent_version]
            for sub_key in key:
                self.paths_ordering[sub_key] = paths_ordering
        else:
            calendar = _obtain_unique_calendar(self.paths_ordering,
                                                   semaphores=self.semaphores,
                                                   time_var=self.time_var,
                                                   session=self.session,
                                                   remote_netcdf_kwargs=self.remote_netcdf_kwargs)
            date_axis, table, units = _obtain_date_axis(paths_ordering,
                                                        self.time_frequency,
                                                        self.is_instant,
                                                        calendar,
                                                        semaphores=self.semaphores,
                                                        time_var=self.time_var,
                                                        session=self.session,
                                                        remote_netcdf_kwargs=self.remote_netcdf_kwargs)

            if len(table['paths'])>0:
                #Convert time axis to numbers and find the unique time axis:
                time_axis, time_axis_unique, date_axis_unique = _unique_time_axis(date_axis,units,calendar,self.year,self.month)

                paths_ordering, paths_id_on_time_axis = _reduce_paths_ordering(time_axis,time_axis_unique,paths_ordering,table)

                #Create soft links:
                paths_id_list = [path_id for path_id in paths_ordering['path_id'] ]

            for sub_key in key:
                self.paths_ordering[sub_key] = paths_ordering

                self.paths_link[sub_key] = np.array((len(time_axis_unique),), dtype=np.int64)
                self.indices_link[sub_key] = np.array((len(time_axis_unique),), dtype=np.int64)
                for time_id, time in enumerate(time_axis_unique):
                    #For each time in time_axis_unique, pick path_id in paths_id_list. They
                    #should all be the same. Pick the first one:
                    paths_id_that_can_be_used = np.unique([path_id for path_id in paths_id_on_time_axis[time==time_axis]
                                                    if path_id in paths_id_list])
                    path_id_to_use = [path_id for path_id in paths_id_list
                                        if path_id in paths_id_that_can_be_used][0]
                    self.paths_link[sub_key][time_id] = path_id_to_use
                    self.indices_link[sub_key][time_id] = table[indices_dim][np.logical_and(paths_id_on_time_axis == path_id_to_use, time == time_axis)][0]
                
        return 

    def record_paths(self, output, var):
        return self.create(output)

    def record_meta_data(self, output, var):
        #Retrieve time and meta:
        self.create_variable(output,var)
        #Put version:
        output.setncattr(str('netcdf_soft_links_version'),str('1.3'))
        return

    def _record_fx(self,output,var):

        queryable_paths_list = [item for item in self.usable_paths_list if item['file_type'] in _queryable_file_types]
        if len(queryable_paths_list)==0:
            temp_file_handle, temp_file_name = tempfile.mkstemp()
        try:
            if len(queryable_paths_list)==0:
                path = self.usable_paths_list[0]
                #Download the file to temp
                #retrieval_utils.download_secure(path['path'].split('|')[0],
                #                temp_file_name,
                #                path['file_type'],
                #                username=username,user_pass=user_pass)
                #remote_data=remote_netcdf.remote_netCDF(temp_file_name,path['file_type'],self.semaphores,**self.remote_netcdf_kwargs)
            else:
                #Check if data in available:
                path = queryable_paths_list[0]

                remote_data = remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],semaphores=self.semaphores,
                                                                                                     session=self.session,
                                                                                                     **self.remote_netcdf_kwargs)
                alt_path_name = remote_data.check_if_available_and_find_alternative([item['path'].split('|')[0] for item in queryable_paths_list],
                                                                            [item['file_type'] for item in queryable_paths_list],
                                                                         [item['path'].split('|')[_unique_file_id_list.index('checksum')+1] for item in queryable_paths_list],
                                                                         _queryable_file_types)

                #Use aternative path:
                path = queryable_paths_list[[item['path'].split('|')[0] for item in queryable_paths_list].index(alt_path_name)]
                remote_data = remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],semaphores=self.semaphores,
                                                                                                     session=self.session,
                                                                                                     **self.remote_netcdf_kwargs)

            output = remote_data.safe_handling(netcdf_utils.retrieve_variables,output,zlib=True)

            for att in path.keys():
                if att!='path':      
                    output.setncattr(att,path[att])
            output.setncattr('path',path['path'].split('|')[0])
            for unique_file_id in _unique_file_id_list:
                output.setncattr(unique_file_id,path['path'].split('|')[_unique_file_id_list.index(unique_file_id)+1])
        finally:
            if len(queryable_paths_list)==0:
                os.remove(temp_file_name)

        #Create soft links
        output_grp = self._create_output(output, var)
        output_grp.createVariable(var, np.float32, (), zlib=True)
        return

    def _create_output(self, output, var):
        if not 'soft_links' in output.groups.keys():
            output_grp = output.createGroup('soft_links')
        else:
            output_grp = output.groups['soft_links']

        #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
        output_grp.createDimension('path',None)
        for id in ['version','path_id']:
            output_grp.createVariable(id,np.int64,('path',),chunksizes=(1,),zlib=True)[:]=self.paths_ordering[id]
        for id in _id_list:
            temp = output_grp.createVariable(id,str,('path',),chunksizes=(1,),zlib=True)
            for file_id, file in enumerate(self.paths_ordering[var]['path']):
                temp[file_id] = str(self.paths_ordering[var][id][file_id])
        return output_grp

    def _create_variable(self, output, var):
        #Recover time axis for all files:

        if len(table['paths'])>0:
            #Load data
            queryable_file_types_available = list(set(self.paths_ordering['file_type']).intersection(_queryable_file_types))
            if len(queryable_file_types_available)>0:
                #Open the first file and use its metadata to populate container file:
                first_id = list(self.paths_ordering['file_type']).index(queryable_file_types_available[0])
                remote_data = remote_netcdf.remote_netCDF(self.paths_ordering['path'][first_id],
                                                        self.paths_ordering['file_type'][first_id],
                                                        semaphores=self.semaphores,
                                                        session=self.session,
                                                        **self.remote_netcdf_kwargs)
                time_dim,output = remote_data.safe_handling(netcdf_utils.find_time_dim_and_replicate_netcdf_file,output,time_var=self.time_var)
            else:
                remote_data = remote_netcdf.remote_netCDF(self.paths_ordering['path'][0],
                                                        self.paths_ordering['file_type'][0],
                                                        semaphores=self.semaphores,
                                                        session=self.session,
                                                        **self.remote_netcdf_kwargs)
                time_dim = self.time_var

            #Create time axis in ouptut:
            netcdf_utils.create_time_axis_date(output,date_axis_unique,units,self.calendar,time_dim=time_dim)

            self.create(output)
            if isinstance(var,list):
                for sub_var in var:
                    output = _record_indices(self.paths_ordering,
                                               remote_data,output,sub_var,
                                               time_dim,time_axis,time_axis_unique,
                                               table,paths_id_on_time_axis,self.record_other_vars)
            else:
                output = _record_indices(self.paths_ordering,
                                           remote_data,output,var,
                                           time_dim,time_axis,time_axis_unique,
                                           table,paths_id_on_time_axis,self.record_other_vars)
        return

def _record_indices(paths_ordering,
                    remote_data,output,var,
                    time_dim,time_axis,time_axis_unique,
                    table,paths_id_on_time_axis,record_other_vars):
    #Create descriptive vars:
    #Must use compression, especially for ocean variables in curvilinear coordinates:
    remote_data.safe_handling(netcdf_utils.retrieve_variables_no_time,output,time_dim,zlib=True)

    #CREATE LOOK-UP TABLE:
    output_grp = output.groups['soft_links']
    indices_dim = 'indices'
    if not indices_dim in output_grp.dimensions:
        output_grp.createDimension(indices_dim,2)
    if not indices_dim in output_grp.variables.keys():
        output_grp.createVariable(indices_dim,np.str,(indices_dim,),chunksizes=(1,))
    indices = output_grp.variables[indices_dim]
    indices[0] = 'path'
    indices[1] = time_dim

    #Replicate variable in main group:
    remote_data.safe_handling(netcdf_utils.replicate_netcdf_var,output,var,zlib=True)

    if var in output.variables.keys():
        var_out = output_grp.createVariable(var,np.int64,(time_dim,indices_dim),zlib=True)

        for time_id, time in enumerate(time_axis_unique):
            #For each time in time_axis_unique, pick path_id in paths_id_list. They
            #should all be the same. Pick the first one:
            paths_id_that_can_be_used = np.unique([path_id for path_id in paths_id_on_time_axis[time==time_axis]
                                            if path_id in paths_id_list])
            path_id_to_use = [path_id for path_id in paths_id_list
                                if path_id in paths_id_that_can_be_used][0]
            var_out[time_id,0] = path_id_to_use
            var_out[time_id,1] = table[indices_dim][np.logical_and(paths_id_on_time_axis==path_id_to_use,time==time_axis)][0]
        if np.ma.count_masked(var_out)>0:
            raise ValueError('Variable was not created properly. Must recreate')

    #Create support variables:
    if record_other_vars:
        previous_output_variables_list = output.variables.keys()
        #Replicate other vars:
        output = remote_data.safe_handling(netcdf_utils.replicate_netcdf_other_var,output,var,time_dim)
        output_variables_list = [ other_var for other_var in netcdf_utils.variables_list_with_time_dim(output,time_dim)
                                    if other_var!=var]
        for other_var in output_variables_list:
            if (not other_var in previous_output_variables_list):
                var_out = output_grp.createVariable(other_var,np.int64,(time_dim,indices_dim),zlib=True)
                #Create soft links:
                for time_id, time in enumerate(time_axis_unique):
                    #For each time in time_axis_unique, pick path_id in paths_id_list. They
                    #should all be the same. Pick the first one:
                    paths_id_that_can_be_used = np.unique([path_id for path_id in paths_id_on_time_axis[time==time_axis]
                                                    if path_id in paths_id_list])
                    path_id_to_use = [path_id for path_id in paths_id_list
                                        if path_id in paths_id_that_can_be_used][0]
                    var_out[time_id,0] = path_id_to_use
                    var_out[time_id,1] = table[indices_dim][np.logical_and(paths_id_on_time_axis==path_id_to_use,time==time_axis)][0]
                if np.ma.count_masked(var_out)>0:
                    raise ValueError('Variable was not created properly. Must recreate')
    output.sync()
    return output

def _order_paths_by_preference(paths_list,file_type_list,data_node_list,check_dimensions=True,
                                semaphores=dict(),time_var='time',session=None,remote_netcdf_kwargs=dict()):
    if check_dimensions:
        #Use this option to ensure some sort of dimensional compatibility:
        sorts_list = ['version', 'file_type_id', 'dimension_type_id', 'data_node_id', 'path_id']
    else:
        sorts_list = ['version', 'file_type_id', 'data_node_id', 'path_id']

    #FIND ORDERING:
    paths_desc = []
    for id in sorts_list:
        paths_desc.append((id,np.int64))
    for id in _id_list:
        paths_desc.append((id,'a255'))
    paths_ordering = np.empty((len(paths_list),), dtype=paths_desc)

    if check_dimensions:
        dimension_type_list = ['unqueryable',]

    for file_id, file in enumerate(paths_list):
        paths_ordering['path'][file_id] = file['path'].split('|')[0]
        #Convert path name to 'unique' integer using hash.
        #The integer will not really be unique but collisions
        #should be extremely rare for similar strings with only small variations.
        paths_ordering['path_id'][file_id] = hash(
                                                paths_ordering['path'][file_id]
                                                    )
        for unique_file_id in _unique_file_id_list:
            paths_ordering[unique_file_id][file_id] = file['path'].split('|')[_unique_file_id_list.index(unique_file_id)+1]
        paths_ordering['version'][file_id] = np.long(file['version'][1:])

        paths_ordering['file_type'][file_id] = file['file_type']
        paths_ordering['data_node'][file_id] = remote_netcdf.get_data_node(file['path'],paths_ordering['file_type'][file_id])
        
        if check_dimensions:
            #Dimensions types. Find the different dimensions types:
            if not paths_ordering['file_type'][file_id] in _queryable_file_types:
                paths_ordering['dimension_type_id'][file_id] = dimension_type_list.index('unqueryable')
            else:
                remote_data = remote_netcdf.remote_netCDF(paths_ordering['path'][file_id],
                                                        paths_ordering['file_type'][file_id],
                                                        semaphores=semaphores,
                                                        session=session,
                                                        **remote_netcdf_kwargs)
                dimension_type = remote_data.safe_handling(netcdf_utils.find_dimension_type,time_var=time_var)
                if not dimension_type in dimension_type_list: dimension_type_list.append(dimension_type)
                paths_ordering['dimension_type_id'][file_id] = dimension_type_list.index(dimension_type)

    if check_dimensions:
        #Sort by increasing number. Later when we sort, we should get a uniform type:
        dimension_type_list_number = [ sum(paths_ordering['dimension_type_id']==dimension_type_id)
                                        for dimension_type_id,dimension_type in enumerate(dimension_type_list)]
        sort_by_number = np.argsort(dimension_type_list_number)[::-1]
        paths_ordering['dimension_type_id'] = sort_by_number[paths_ordering['dimension_type_id']]

    #Sort paths from most desired to least desired:
    #First order desiredness for least to most:
    data_node_order = copy.copy(data_node_list)[::-1]
    file_type_order = copy.copy(file_type_list)[::-1]
    for file_id, file in enumerate(paths_list):
        paths_ordering['data_node_id'][file_id] = data_node_order.index(paths_ordering['data_node'][file_id])
        paths_ordering['file_type_id'][file_id] = file_type_order.index(paths_ordering['file_type'][file_id])
    #'version' is implicitly from least to most

    #sort and reverse order to get from most to least:
    return np.sort(paths_ordering,order = sorts_list)[::-1]

def _recover_date(path,time_frequency,is_instant,calendar,semaphores=dict(),time_var='time',session=None,remote_netcdf_kwargs=dict()):
    file_type = path['file_type']
    path_name = str(path['path']).split('|')[0]
    remote_data = remote_netcdf.remote_netCDF(path_name,
                                            file_type,
                                            semaphores=semaphores,
                                            session=session,
                                            **remote_netcdf_kwargs)
    date_axis = remote_data.get_time(time_frequency=time_frequency,
                                    is_instant=is_instant,
                                    time_var=time_var,
                                    calendar=calendar)
    time_units = remote_data.get_time_units(calendar,time_var=time_var)
    table_desc = [
               ('paths','a255'),
               ('file_type','a255'),
               ('time_units','a255'),
               ('indices','int64')
               ] + [(unique_file_id,'a255') for unique_file_id in _unique_file_id_list]
    if len(date_axis)>0:
        table = np.empty(date_axis.shape, dtype=table_desc)
        if len(date_axis)>0:
            table['paths'] = np.array([str(path_name) for item in date_axis])
            table['file_type'] = np.array([str(file_type) for item in date_axis])
            table['time_units'] = np.array([str(time_units) for item in date_axis])
            table['indices'] = range(0,len(date_axis))
            for unique_file_id in _unique_file_id_list:
                table[unique_file_id] = np.array([str(path[unique_file_id]) for item in date_axis])
        return date_axis,table
    else:
        #No time axis, return empty arrays:
        return np.array([]),np.array([], dtype=table_desc)

def _obtain_date_axis(paths_ordering,time_frequency,is_instant,calendar,semaphores=dict(),time_var='time',session=None,remote_netcdf_kwargs=dict()):
    #Retrieve time axes from queryable file types or reconstruct time axes from time stamp
    #from non-queryable file types.
    date_axis, table =  map(np.concatenate,
                    zip(*map(lambda x:_recover_date(x,time_frequency,
                                                      is_instant,
                                                      calendar,
                                                      semaphores=semaphores,
                                                      time_var=time_var,
                                                      session=session,
                                                      remote_netcdf_kwargs=remote_netcdf_kwargs),np.nditer(paths_ordering))))
    if len(date_axis)>0:
        #If all files have the same time units, use this one. Otherwise, create a new one:
        unique_date_units=np.unique(table['time_units'])
        if len(unique_date_units)==1:
            units = unique_date_units[0]
        else:
            units = 'days since '+str(np.sort(date_axis)[0])
        if units==None:
            units = 'days since '+str(np.sort(date_axis)[0])
    return date_axis,table,units

def _recover_calendar(path,semaphores=dict(),time_var='time',session=None,remote_netcdf_kwargs=dict()):
    file_type = path['file_type']
    path_name = str(path['path']).split('|')[0]
    remote_data = remote_netcdf.remote_netCDF(path_name,
                                            file_type,
                                            semaphores=semaphores,
                                            session=session,
                                            **remote_netcdf_kwargs)
    calendar = remote_data.get_calendar(time_var=time_var)
    return calendar, file_type 

def _obtain_unique_calendar(paths_ordering,semaphores=dict(),time_var='time',session=None,remote_netcdf_kwargs=dict()):
    calendar_list,file_type_list=zip(*map(lambda x:_recover_calendar(x,semaphores=semaphores,
                                                                        time_var=time_var,
                                                                        session=session,
                                                                        remote_netcdf_kwargs=remote_netcdf_kwargs),np.nditer(paths_ordering)))
    #Find the calendars found from queryable file types:
    calendars = set([item[0] for item in zip(calendar_list,file_type_list) if item[1] in _queryable_file_types])
    if len(calendars)==1:
        return calendars.pop()
    return calendar_list[0]

def _reduce_paths_ordering(time_axis,time_axis_unique,paths_ordering,table):
    #CREATE LOOK-UP TABLE:
    paths_indices_on_time_axis = np.empty(time_axis.shape,dtype=np.int64)
    paths_id_on_time_axis = np.empty(time_axis.shape,dtype=np.int64)

    paths_list = list(paths_ordering['path'])
    paths_id_list = list(paths_ordering['path_id'])
    #for path_id, path in zip(paths_id_list,paths_list):
    for path_index, (path_id, path) in enumerate(zip(paths_id_list,paths_list)):
        #find in table the path and assign path_index to it:
        paths_indices_on_time_axis[path==table['paths']] = path_index
        paths_id_on_time_axis[path==table['paths']] = path_id

    #Remove paths that are not necessary over the requested time range:
    #First, list the paths_id used:
    #Pick the lowest path index so that we follow the paths_ordering:
    useful_paths_id_list_unique = list(
                                np.unique(
                                    [paths_id_list[np.min(paths_indices_on_time_axis[time==time_axis])] 
                                        for time_id, time in enumerate(time_axis_unique)]))

    #Second, list the path_names corresponding to these paths_id:
    #useful_file_name_list=[useful_path_id.split('/')[-1] for useful_path_id in 
    #                        [path for path_id, path in zip(paths_id_list,paths_list)
    #                                if path_id in useful_paths_id_list_unique] ]
    useful_file_name_list_unique = [paths_list[paths_id_list.index(path_id)].split('/')[-1]
                                        for path_id in useful_paths_id_list_unique]

    #Find the indices to keep:
    useful_file_id_list = [file_id for file_id, file in enumerate(paths_ordering)
                            if paths_ordering['path_id'][file_id] in useful_paths_id_list_unique]
                            
    #Finally, check if some equivalent indices are worth keeping:
    for file_id, file in enumerate(paths_ordering):
        if not paths_ordering['path_id'][file_id] in useful_paths_id_list_unique:
            #This file was not kept but it might be the same data, in which
            #case we would like to keep it.
            #Find the file name (remove path):
            file_name = paths_ordering['path'][file_id].split('/')[-1]
            if file_name in useful_file_name_list_unique:
                #If the file name is useful, find its path_id: 
                equivalent_path_id = useful_paths_id_list_unique[useful_file_name_list_unique.index(file_name)]
                    
                #Use this to find its file_id:
                equivalent_file_id = list(paths_ordering['path_id']).index(equivalent_path_id)
                #Then check if the checksum are the same. If yes, keep the file!
                if paths_ordering['checksum'][file_id]==paths_ordering['checksum'][equivalent_file_id]:
                    useful_file_id_list.append(file_id)
            
    #Sort paths_ordering:
    if len(useful_file_id_list)>0:
        return paths_ordering[np.sort(useful_file_id_list)],paths_id_on_time_axis
    else:   
        return paths_ordering,paths_id_on_time_axis

def _unique_time_axis(date_axis,units,calendar,year,month):
    time_axis = netcdf_utils.get_time_axis_relative(date_axis,units,calendar)
    time_axis_unique = np.unique(time_axis)

    date_axis_unique = netcdf_utils.get_date_axis_relative(time_axis_unique,units,calendar)

    #Include a filter on year and month: 
    time_desc = {}
    if year!=None:
        if year[0]<10:
            #This is important for piControl
            temp_year = list(np.array(year)+np.min([date.year for date in date_axis_unique]))
            #min_year = np.min([date.year for date in date_axis_unique])
        else:
            temp_year = year
        if month!=None:
            valid_times = np.array([True if (date.year in temp_year and 
                                     date.month in month) else False for date in  date_axis_unique])
        else:
            valid_times = np.array([True if date.year in temp_year else False for date in  date_axis_unique])
    else:
        if month!=None:
            valid_times = np.array([True if date.month in month else False for date in  date_axis_unique])
        else:
            valid_times = np.array([True for date in  date_axis_unique])
        
    #self.time_axis_unique = time_axis_unique[valid_times]
    #self.date_axis_unique = date_axis_unique[valid_times]
    #self.time_axis = time_axis
    return time_axis, time_axis_unique[valid_times], date_axis_unique[valid_times]
