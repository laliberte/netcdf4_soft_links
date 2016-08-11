#External:
import numpy as np
import netCDF4
import os
import copy
import datetime
import tempfile
from collections import OrderedDict, Mapping

#Internal:
import remote_netcdf
import http_netcdf
import netcdf_utils
import indices_utils
import queues_manager

file_unique_id_list=['checksum_type','checksum','tracking_id']

class Dataset(Mapping):
    def __init__(self,
                 data_root,
                 file_type_list=[], data_node_list=[],
                 time_frequency=None,
                 check_dimensions=False,
                 download_all_files=False,
                 download_all_opendap=False,
                 min_year=None,
                 year=None,
                 month=None,
                 day=None,
                 hour=None,
                 previous=0,
                 next=0,
                 time_var='time',
                 q_manager=None,
                 session=None,
                 remote_netcdf_kwargs={}):

        self.data_root = data_root
        self.q_manager = q_manager
        self.remote_netcdf_kwargs = remote_netcdf_kwargs
        self.session = session

        self.download_all_files = download_all_files
        self.download_all_opendap = download_all_opendap

        self.paths_sent_for_retrieval=[]

        self.time_var = netcdf_utils.find_time_var(self.data_root, time_var=time_var)
        if (self.time_var !=None and 
            len(self.data_root.variables[self.time_var])>0):
            #Then find time axis, time restriction and which variables to retrieve:
            date_axis = netcdf_utils.get_date_axis(self.data_root, self.time_var)
            time_axis = self.data_root.variables[self.time_var][:]

            self.time_restriction = _get_time_restriction(date_axis,
                                                       min_year=min_year, years=year, 
                                                       months=month, days=day, 
                                                       hours=hour, previous=previous, next=next)
            #time sorting:
            self.time_restriction_sort = np.argsort(date_axis[self.time_restriction])

            #Sorted time_axis:
            self.date_axis = date_axis[self.time_restriction_sort]
            self.time_axis = time_axis[self.time_restriction_sort]
        else:
            self.time_axis,self.date_axis, self.time_restriction, self.time_restriction_sort = np.array([]), np.array([]), np.array([]), np.array([])

        #Set retrieveable variables:
        if 'soft_links' in self.data_root.groups:
            #Initialize variables:
            retrievable_vars=[var for var in self.data_root.variables 
                                if  ( var in self.data_root.groups['soft_links'].variables and
                                      var != self.time_var)]

            #Get list of paths:
            for path_desc in ['path','path_id','file_type','version']+file_unique_id_list:
                setattr(self,path_desc+'_list',self.data_root.groups['soft_links'].variables[path_desc][:])
        else:
            retrievable_vars=[var for var in self.data_root.variables]

        self.paths_link = OrderedDict()
        self.indices_link = OrderedDict()

        for var_to_retrieve in retrievable_vars:
            # Determine the paths_ids for soft links:
            if self.data_root.groups['soft_links'].variables[var_to_retrieve].shape[0]==1:
                #Prevents a bug in h5py when self.data_root is an h5netcdf file:
                if np.all(self.time_restriction):
                    self.paths_link[var_to_retrieve] = self.data_root.groups['soft_links'].variables[var_to_retrieve][:,0]
                    self.indices_link[var_to_retrieve] = self.data_root.groups['soft_links'].variables[var_to_retrieve][:,1]
            else:
                self.paths_link[var_to_retrieve] = (self.data_root.
                                               groups['soft_links'].
                                               variables[var_to_retrieve][self.time_restriction,0][self.time_restriction_sort])
                self.indices_link[var_to_retrieve] = (self.data_root.
                                                      groups['soft_links'].
                                                      variables[var_to_retrieve][self.time_restriction,1][self.time_restriction_sort])

        return

    def replicate(self, output, check_empty=False, chunksize=None):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(self.data_root, output)
        #replicate and copy variables:
        for var_name in self.data_root.variables:
            netcdf_utils.replicate_and_copy_variable(self.data_root, output, var_name, 
                                                     check_empty=check_empty, zlib=True, chunksize=chunksize)
        if 'soft_links' in self.data_root.groups:
            output_grp=netcdf_utils.replicate_group(self.data_root, output, 'soft_links')
            netcdf_utils.replicate_netcdf_file(self.data_root.groups['soft_links'], output_grp)
            for var_name in self.data_root.groups['soft_links'].variables:
                netcdf_utils.replicate_and_copy_variable(self.data_root.groups['soft_links'], output_grp, var_name,
                                                         check_empty=check_empty, zlib=True, chunksize=chunksize)
        return

    def append_to(self,output,check_empty=False):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(self.data_root,output)
    
        record_dimensions=netcdf_utils.append_record(self.data_root,output)
        #replicate and copy variables:
        for var_name in self.data_root.variables:
            if not var_name in record_dimensions:
                if ( var_name in output.variables and
                      netcdf_utils.check_dimensions_compatibility(self.data_root,output,var_name,exclude_unlimited=True) and
                      len(record_dimensions)>0):
                    #Variable can be appended along some record dimensions:
                    netcdf_utils.append_and_copy_variable(self.data_root, output, var_name, record_dimensions, check_empty=check_empty)
                elif ( not var_name in output.variables and 
                      netcdf_utils.check_dimensions_compatibility(self.data_root, output, var_name)):
                    #Variable can be copied:
                    netcdf_utils.replicate_and_copy_variable(self.data_root, output, var_name, check_empty=check_empty)
    
        if 'soft_links' in self.data_root.groups:
            data_grp=self.data_root.groups['soft_links']
            output_grp=netcdf_utils.replicate_group(self.data_root,output,'soft_links')
            netcdf_utils.replicate_netcdf_file(self.data_root.groups['soft_links'],output_grp)
    
            record_dimensions.update(netcdf_utils.append_record(data_grp,output_grp))
            for var_name in data_grp.variables:
                if not var_name in record_dimensions:
                    if ( var_name in output_grp.variables and
                          netcdf_utils.check_dimensions_compatibility(data_grp, output_grp, var_name, exclude_unlimited=True)):
                        #Variable can be appended along the time and path dimensions:
                        netcdf_utils.append_and_copy_variable(data_grp, output_grp, var_name, record_dimensions, check_empty=check_empty)
                    elif ( not var_name in output_grp.variables and 
                          netcdf_utils.check_dimensions_compatibility(data_grp, output_grp, var_name)):
                        #Variable can be copied:
                        netcdf_utils.replicate_and_copy_variable(data_grp, output_grp, var_name, check_empty=check_empty)
        return

    def retrieve(self, output, retrieval_type, out_dir='.'):
        if self.time_var!=None:
            #Get the dataset dimensions:
            self.dimensions, self.unsort_dimensions = _get_dimensions_slicing(self.data_root,
                                                                              var_to_retrieve,
                                                                              self.time_var)

            #Record to output if output is a netCDF4 Dataset:
            if not self.time_var in output.dimensions:
                #pick only requested times and sort them
                netcdf_utils.create_time_axis(self.data_root, output, 
                                              self.time_axis[self.time_restriction][self.time_restriction_sort],
                                              time_var=self.time_var)

            #Replicate all the other variables:
            for var in set(self.data_root.variables).difference(self.retrievable_vars):
                if not var in output.variables:
                    output = netcdf_utils.replicate_and_copy_variable(self.data_root, output, var)

            if self.retrieval_type in ['download_files','download_opendap']:
                #Replicate soft links for remote_queryable data:
                output_grp = netcdf_utils.replicate_group(self.data_root, output, 'soft_links')
                for var_name in self.data_root.groups['soft_links'].variables:
                    netcdf_utils.replicate_netcdf_var(self.data_root.groups['soft_links'], output_grp, var_name)
                    if ( var_name != self.time_var and 
                         sum(self.time_restriction)>0 ):
                        if self.time_var in self.data_root.groups['soft_links'].variables[var_name].dimensions:
                            if self.data_root.groups['soft_links'].variables[var_name].shape[0] == 1:
                                #Prevents a bug in h5py when self.data_root is an h5netcdf file:
                                if np.all(self.time_restriction):
                                    output_grp.variables[var_name][:] = (self.data_root.groups['soft_links']
                                                                                  .variables[var_name][...])
                            else:
                                output_grp.variables[var_name][:] = (self.data_root.groups['soft_links']
                                                                                  .variables[var_name]
                                                                                            [self.time_restriction,...]
                                                                                            [self.time_restriction_sort,...])
                        else:
                            output_grp.variables[var_name][:] = self.data_root.groups['soft_links'].variables[var_name][:]

            #Define tree:
            tree = output.path.split('/')[1:]

            for var_to_retrieve in self.retrievable_vars:
                #Replicate variable to output:
                netcdf_utils.replicate_netcdf_var(self.data_root, output, var_to_retrieve, chunksize=-1, zlib=True)

                self.retrieve_var(var_to_retrieve, output.variables[var_to_retrieve], 
                                  output.variables[var_to_retrieve].dimensions , retrieval_type, out_dir=out_dir, tree=tree)
        else:
            #Fixed variable. Do not retrieve, just copy:
            for var_to_retrieve in self.retrievable_vars:
                output = netcdf_utils.replicate_and_copy_variable(self.data_root, output, var_to_retrieve)
        return

    def retrieve_var(self, var_to_retrieve, out_var, var_dimensions, retrieval_type, out_dir='.', tree=[]):
        if sum(self.time_restriction)==0:
            return

        #Use search sorted:
        var_paths_link = np.argsort(self.path_id_list)[np.searchsorted(self.path_id_list[var_to_retrieve],
                                                                       self.paths_link[var_to_retrieve],
                                                                       sorter=np.argsort(self.path_id_list[var_to_retrieve]))]

        dimensions = copy.copy(self.dimensions)
        unsort_dimensions = copy.copy(self.unsort_dimensions)
        self._retrieve_all_paths_to_variable(var_paths_link, var_to_retrieve, out_var, var_dimensions,
                                             dimensions, unsort_dimensions,
                                             retrieval_type, out_dir=out_dir, tree=tree)
        return

    #def __getitem__(key):
        
                                            
    def _retrieve_all_paths_to_variable(self, var_paths_link, var_to_retrieve, out_var, var_dimensions,
                                        dimensions, unsort_dimensions,
                                        retrieval_type, out_dir=out_dir, tree=tree):
        #Sort the paths so we query each only once:
        unique_path_id_list, sorting_paths = np.unique(var_paths_link, return_inverse=True)

        for unique_path_id, path_id in enumerate(unique_path_id_list):
            self._retrieve_path_to_variable(unique_path_id, path_id, sorting_paths, var_to_retrieve, out_var,
                                            var_dimensions, dimensions, unsort_dimensions, 
                                            retrieval_type, out_dir=out_dir, tree=tree)
        return

    def _retrieve_path_to_variable(self, unique_path_id, path_id, sorting_paths, var_to_retrieve, out_var,
                                   var_dimensions, dimensions, unsort_dimensions,
                                   retrieval_type, out_dir='.', tree=[]):
        path_to_retrieve = self.path_list[path_id]

        #Next, we check if the file is available. If it is not we replace it
        #with another file with the same checksum, if there is one!
        file_type = self.file_type_list[list(self.path_list).index(path_to_retrieve)]

        semaphores=dict()
        if 'semaphores' in dir(self.q_manager):
            semaphores=self.q_manager.semaphores

        remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,
                                                file_type,
                                                semaphores=semaphores,
                                                session=self.session,
                                                **self.remote_netcdf_kwargs)

        #See if the available path is available for download and find alternative:
        if retrieval_type=='download_files':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                 self.file_type_list,
                                                                                 self.checksum_list,
                                                                                 remote_netcdf.downloadable_file_types,
                                                                                 num_trials=2)
        elif retrieval_type=='download_opendap':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                 self.file_type_list,
                                                                                 self.checksum_list,
                                                                                 remote_netcdf.remote_queryable_file_types,
                                                                                 num_trials=2)
        elif retrieval_type=='load':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                 self.file_type_list,
                                                                                 self.checksum_list,
                                                                                 remote_netcdf.local_queryable_file_types,
                                                                                 num_trials=2)
        elif retrieval_type=='assign':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                 self.file_type_list,
                                                                                 self.checksum_list,
                                                                                 remote_netcdf.queryable_file_types,
                                                                                 num_trials=2)

        if path_to_retrieve is None:
            #Do not retrieve!
            return

        #See if there is a better file_type available:
        if (retrieval_type == 'download_files' and 
            not self.download_all_files):
            alt_path_to_retrieve = remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                     self.file_type_list,
                                                                                     self.checksum_list,
                                                                                     remote_netcdf.remote_queryable_file_types+
                                                                                     remote_netcdf.local_queryable_file_types,
                                                                                     num_trials=2)
            #Do not retrieve if a 'better' file type exists and is available
            if alt_path_to_retrieve!=None: return
        elif (retrieval_type=='download_opendap' and 
              not self.download_all_opendap):
            alt_path_to_retrieve = remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                     self.file_type_list,
                                                                                     self.checksum_list,
                                                                                     remote_netcdf.local_queryable_file_types,
                                                                                     num_trials=2)
            #Do not retrieve if a 'better' file type exists and is available
            if alt_path_to_retrieve!=None: return
        elif retrieval_type == 'assign':
            #Use local path, if available:
            path_to_retrieve = remote_data.check_if_available_and_find_alternative(self.path_list,
                                                                                 self.file_type_list,
                                                                                 self.checksum_list,
                                                                                 remote_netcdf.local_queryable_file_types,
                                                                                 num_trials=2)
            

        #Get the file_type, checksum and version of the file to retrieve:
        path_index = list(self.path_list).index(path_to_retrieve)
        file_type = self.file_type_list[path_index]
        version = 'v'+str(self.version_list[path_index])
        checksum = self.checksum_list[path_index]
        checksum_type = self.checksum_type_list[path_index]

        #Reverse pick time indices correponsing to the unique path_id:
        if file_type=='soft_links_container':
            #if the data is in the current file, the data lies in the corresponding time step:
            time_indices = np.arange(len(sorting_paths), dtype=int)[sorting_paths == unique_path_id]
        else:
            time_indices = self.indices_link[var_to_retrieve][sorting_paths == unique_path_id]

        download_args=(0, path_to_retrieve, file_type, var_to_retrieve, tree)

        if retrieval_type != 'download_files':
            #This is an important test that should be included in future releases:
            #with netCDF4.Dataset(path_to_retrieve.split('|')[0]) as data_test:
            #    data_date_axis=netcdf_utils.get_date_axis(data_test,'time')[time_indices]
            #print(path_to_retrieve,self.date_axis[self.time_restriction][self.time_restriction_sort][sorting_paths==unique_path_id],data_date_axis)

            dimensions[self.time_var], unsort_dimensions[self.time_var] = indices_utils.prepare_indices(time_indices)

            if ( retrieval_type == 'download_opendap' and
                 isinstance(out_var, netCDF4.Variable) ):
                new_path = 'soft_links_container/'+os.path.basename(self.path_list[path_index])
                new_file_type = 'soft_links_container'
                self._add_path_to_soft_links(new_path, new_file_type, path_index, sorting_paths==unique_path_id, out_var.group.groups['soft_links'], var_to_retrieve)

            sort_table=np.arange(len(sorting_paths))[sorting_paths==unique_path_id]
            download_kwargs={'dimensions'dimensions,
                             'unsort_dimensions':unsort_dimensions,
                             'sort_table':sort_table
                             }

            if file_type=='soft_links_container':
                #The data is already in the file, we must copy it!
                max_request=2048
                retrieved_data=netcdf_utils.retrieve_container(self.data_root,
                                                                var_to_retrieve,
                                                                dimensions,
                                                                unsort_dimensions,
                                                                sort_table,max_request)
                _assign_leaf(out_var, retrieved_data, sort_table)
            elif file_type in remote_netcdf.remote_queryable_file_types:
                data_node=remote_netcdf.get_data_node(path_to_retrieve,file_type)
                #Send to the download queue:
                self.q_manager.put_to_data_node(data_node,download_args+(download_kwargs,))
            elif file_type in remote_netcdf.local_queryable_file_types:
                #Load and simply assign:
                remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,file_type)
                retrieved_data, sort_table, var_tree = remote_data.download(*download_args[3:],download_kwargs=download_kwargs)
                _assign_leaf(out_var, retrieved_data, sort_table)
        else:
            if path_to_retrieve in self.paths_sent_for_retrieval:
                #Do not download twice!
                return
            else:
                new_path=http_netcdf.destination_download_files(self.path_list[path_index],
                                                                out_dir,
                                                                var_to_retrieve,
                                                                self.path_list[path_index],
                                                                tree)
                new_file_type='local_file'
                if isinstance(out_var, netCDF4.Variable):
                    self._add_path_to_soft_links(new_path, new_file_type, path_index, 
                                                 sorting_paths == unique_path_id, 
                                                 out_var.group.groups['soft_links'], var_to_retrieve)
                                             
                download_kwargs={'out_dir' : out_dir,
                                 'version' : version,
                                 'checksum' : checksum,
                                 'checksum_type' : checksum_type}

                #Keep a list of paths sent for retrieval:
                self.paths_sent_for_retrieval.append(path_to_retrieve)

                data_node = remote_netcdf.get_data_node(path_to_retrieve, file_type)
                #Send to the download queue:
                self.q_manager.put_to_data_node(data_node, download_args + (download_kwargs,))
        return 

    def _add_path_to_soft_links(self, new_path, new_file_type, path_index, time_indices_to_replace, output, var_to_retrieve):
        if not new_path in output.variables['path'][:]:
            output.variables['path'][len(output.dimensions['path'])] = new_path
            output.variables['path_id'][-1] = hash(new_path)
            output.variables['file_type'][-1] = new_file_type
            output.variables['data_node'][-1] = remote_netcdf.get_data_node(new_path,output.variables['file_type'][-1])
            for path_desc in ['version']+file_unique_id_list:
                output.variables[path_desc][-1] = getattr(self,path_desc+'_list')[path_index]
        
        output.variables[var_to_retrieve][time_indices_to_replace,0] = output.variables['path_id'][-1]
        return output

def add_previous(time_restriction):
    return np.logical_or(time_restriction,np.append(time_restriction[1:],False))

def add_next(time_restriction):
    return np.logical_or(time_restriction,np.insert(time_restriction[:-1],0,False))

def time_restriction_years(min_year,years,date_axis,time_restriction_any):
    if years!=None:
        years_axis=np.array([date.year for date in date_axis])
        if min_year!=None:
            #Important for piControl:
            time_restriction=np.logical_and(time_restriction_any, [True if year in years else False for year in years_axis-years_axis.min()+min_year])
        else:
            time_restriction=np.logical_and(time_restriction_any, [True if year in years else False for year in years_axis])
        return time_restriction
    else:
        return time_restriction_any

def time_restriction_months(months,date_axis,time_restriction_for_years):
    if months!=None:
        months_axis=np.array([date.month for date in date_axis])
        #time_restriction=np.logical_and(time_restriction,months_axis==month)
        time_restriction=np.logical_and(time_restriction_for_years,[True if month in months else False for month in months_axis])
        return time_restriction
    else:
        return time_restriction_for_years

def time_restriction_days(days,date_axis,time_restriction_any):
    if days!=None:
        days_axis=np.array([date.day for date in date_axis])
        time_restriction=np.logical_and(time_restriction_any,[True if day in days else False for day in days_axis])
        return time_restriction
    else:
        return time_restriction_any
                    
def time_restriction_hours(hours,date_axis,time_restriction_any):
    if hours!=None:
        hours_axis=np.array([date.hour for date in date_axis])
        time_restriction=np.logical_and(time_restriction_any,[True if hour in hours else False for hour in hours_axis])
        return time_restriction
    else:
        return time_restriction_any
                    
def _get_time_restriction(date_axis,min_year=None,years=None,months=None,days=None,hours=None,previous=0,next=0):
    time_restriction=np.ones(date_axis.shape,dtype=np.bool)

    time_restriction=time_restriction_years(min_year,years,date_axis,time_restriction)
    time_restriction=time_restriction_months(months,date_axis,time_restriction)
    time_restriction=time_restriction_days(days,date_axis,time_restriction)
    time_restriction=time_restriction_hours(hours,date_axis,time_restriction)

    if ( (previous>0) or
         (next>0) ):
        sorted_time_restriction=time_restriction[np.argsort(date_axis)]
        if previous>0:
            for prev_num in range(previous):
                sorted_time_restriction=add_previous(sorted_time_restriction)
        if next>0:
            for next_num in range(next):
                sorted_time_restriction=add_next(sorted_time_restriction)
        time_restriction[np.argsort(date_axis)]=sorted_time_restriction
    return time_restriction

def _get_dimensions_slicing(dataset,var,time_var):
    #Set the dimensions:
    dimensions=OrderedDict()
    unsort_dimensions=OrderedDict()

    for dim in dataset.dimensions:
        if dim != time_var:
            if dim in dataset.variables:
                dimensions[dim] = dataset.variables[dim][:]
            else:
                dimensions[dim] = np.arange(len(dataset.dimensions[dim]))
            unsort_dimensions[dim] = None
        else:
            dimensions[dim] = None
            unsort_dimensions[dim] = None
    return dimensions, unsort_dimensions

def _assign_leaf(out_var, val, sort_table):
    out_var[sort_table,...] = val
    return
