#External:
import numpy as np
import netCDF4
import os
import copy
import datetime

#Internal:
import remote_netcdf
import http_netcdf
import netcdf_utils
import indices_utils

file_unique_id_list=['checksum_type','checksum','tracking_id']

class read_netCDF_pointers:
    def __init__(self,data_root,options=None,
                    semaphores=dict(),queues=None,
                    session=None,
                    cache=None,timeout=120,
                    expire_after=datetime.timedelta(hours=1)):
        self.data_root=data_root
        #Queues and semaphores for safe asynchronous retrieval:
        self.semaphores=semaphores
        self.remote_netcdf_kwargs={'cache':cache,'timeout':timeout,'expire_after':expire_after,'session':session}
        self.queues=queues

        for opt in ['username','password']:
            if opt in dir(options):
                setattr(self,opt,getattr(options,opt))
            else:
                setattr(self,opt,None)

        for opt in ['download_all_files','download_all_opendap']:
            if opt in dir(options):
                setattr(self,opt,getattr(options,opt))
            else:
                setattr(self,opt,False)

        #Set retrieveable variables:
        if 'soft_links' in self.data_root.groups.keys():
            #Initialize variables:
            self.retrievable_vars=[var for var in self.data_root.variables.keys() 
                                if  var in self.data_root.groups['soft_links'].variables.keys()]

            #Get list of paths:
            for path_desc in ['path','path_id','file_type','version']+file_unique_id_list:
                setattr(self,path_desc+'_list',self.data_root.groups['soft_links'].variables[path_desc][:])
        else:
            self.retrievable_vars=[var for var in self.data_root.variables.keys()]

        self.time_var=netcdf_utils.find_time_var(self.data_root)
        if self.time_var!=None:
            #Then find time axis, time restriction and which variables to retrieve:
            self.date_axis=netcdf_utils.get_date_axis(self.data_root.variables[self.time_var])
            self.time_axis=self.data_root.variables[self.time_var][:]
            self.time_restriction=get_time_restriction(self.date_axis,options)
            #time sorting:
            self.time_restriction_sort=np.argsort(self.date_axis[self.time_restriction])
        else:
            self.time_axis,self.date_axis, self.time_restriction, self.time_restriction_sort=np.array([]),np.array([]),np.array([]),np.array([])
        return

    def replicate(self,output,hdf5=None,check_empty=False,chunksize=None):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(self.data_root,output)
        #replicate and copy variables:
        for var_name in self.data_root.variables.keys():
            netcdf_utils.replicate_and_copy_variable(self.data_root,output,var_name,hdf5=hdf5,check_empty=check_empty,zlib=True,chunksize=chunksize)
        if 'soft_links' in self.data_root.groups.keys():
            output_grp=netcdf_utils.replicate_group(self.data_root,output,'soft_links')
            netcdf_utils.replicate_netcdf_file(self.data_root.groups['soft_links'],output_grp)
            if hdf5!=None:
                hdf5_grp=hdf5['soft_links']
            else:
                hdf5_grp=hdf5
            for var_name in self.data_root.groups['soft_links'].variables.keys():
                netcdf_utils.replicate_and_copy_variable(self.data_root.groups['soft_links'],output_grp,var_name,hdf5=hdf5_grp,check_empty=check_empty,zlib=True,chunksize=chunksize)
        return

    def append(self,output,hdf5=None,check_empty=False):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(self.data_root,output)

        record_dimensions=netcdf_utils.append_record(self.data_root,output)
        #replicate and copy variables:
        for var_name in self.data_root.variables.keys():
            if not var_name in record_dimensions.keys():
                if ( var_name in output.variables.keys() and
                      netcdf_utils.check_dimensions_compatibility(self.data_root,output,var_name,exclude_unlimited=True) and
                      len(record_dimensions.keys())>0):
                    #Variable can be appended along some record dimensions:
                    netcdf_utils.append_and_copy_variable(self.data_root,output,var_name,record_dimensions,hdf5=hdf5,check_empty=check_empty)
                elif ( not var_name in output.variables.keys() and 
                      netcdf_utils.check_dimensions_compatibility(self.data_root,output,var_name)):
                    #Variable can be copied:
                    netcdf_utils.replicate_and_copy_variable(self.data_root,output,var_name,hdf5=hdf5,check_empty=check_empty)

        if 'soft_links' in self.data_root.groups.keys():
            data_grp=self.data_root.groups['soft_links']
            output_grp=netcdf_utils.replicate_group(self.data_root,output,'soft_links')
            netcdf_utils.replicate_netcdf_file(self.data_root.groups['soft_links'],output_grp)
            if hdf5!=None:
                hdf5_grp=hdf5['soft_links']
            else:
                hdf5_grp=hdf5

            record_dimensions.update(netcdf_utils.append_record(data_grp,output_grp))
            for var_name in data_grp.variables.keys():
                if not var_name in record_dimensions.keys():
                    if ( var_name in output_grp.variables.keys() and
                          netcdf_utils.check_dimensions_compatibility(data_grp,output_grp,var_name,exclude_unlimited=True)):
                        #Variable can be appended:
                        netcdf_utils.append_and_copy_variable(data_grp,output_grp,var_name,record_dimensions,hdf5=hdf5_grp,check_empty=check_empty)
                    elif ( not var_name in output_grp.variables.keys() and 
                          netcdf_utils.check_dimensions_compatibility(data_grp,output_grp,var_name)):
                        #Variable can be copied:
                        netcdf_utils.replicate_and_copy_variable(data_grp,output_grp,var_name,hdf5=hdf5_grp,check_empty=check_empty)
        return

    #def retrieve_without_time(self,retrieval_type,output):
    #    #This function simply retrieves all the files:
    #    self.retrieval_queue_list=[]
    #    file_path=output
    #    for path_to_retrieve in self.path_list:
    #        path_index=list(self.path_list).index(path_to_retrieve)
    #        file_type=self.file_type_list[path_index]
    #        version='v'+str(self.version_list[path_index])
    #        data_node=remote_netcdf.get_data_node(path_to_retrieve,file_type)
    #
    #        #Get the file tree:
    #        args = ({'path':'|'.join([path_to_retrieve,] +
    #                           [ getattr(self,file_unique_id+'_list')[path_index] for file_unique_id in file_unique_id_list]),
    #                'var':self.tree[-1],
    #                'file_path':file_path,
    #                'out_dir':out_dir,
    #                'version':version,
    #                'file_type':file_type,
    #                'data_node':data_node,
    #                'username':self.username,
    #                'user_pass':self.password},
    #                copy.deepcopy(self.tree))
    #
    #        #Retrieve only if it is from the requested data node:
    #        self.retrieval_queue_list.append((getattr(retrieval_utils,retrieval_type),)+copy.deepcopy(args))
    #    return

    def retrieve(self,output,retrieval_type,filepath=None,out_dir='.'):
        #Define tree:
        self.tree=output.path.split('/')[1:]
        self.filepath=filepath
        self.out_dir=out_dir
        self.retrieval_type=retrieval_type

        if self.time_var!=None:
            #Record to output if output is a netCDF4 Dataset:
            if not self.time_var in output.dimensions.keys():
                #pick only requested times and sort them
                netcdf_utils.create_time_axis(self.data_root,output,self.time_axis[self.time_restriction][self.time_restriction_sort])

            #Replicate all the other variables:
            for var in set(self.data_root.variables.keys()).difference(self.retrievable_vars):
                if not var in output.variables.keys():
                    output=netcdf_utils.replicate_and_copy_variable(self.data_root,output,var)

            if self.retrieval_type in ['download_files','download_opendap']:
                #Replicate soft links for remote_queryable data:
                output_grp=netcdf_utils.replicate_group(self.data_root,output,'soft_links')
                for var_name in self.data_root.groups['soft_links'].variables.keys():
                    netcdf_utils.replicate_netcdf_var(self.data_root.groups['soft_links'],output_grp,var_name)
                    if sum(self.time_restriction)>0:
                        if self.time_var in self.data_root.groups['soft_links'].variables[var_name].dimensions:
                            #variable with time, pick only requested times and sort them
                            output_grp.variables[var_name][:]=self.data_root.groups['soft_links'].variables[var_name][self.time_restriction,:][self.time_restriction_sort,:]
                        else:
                            output_grp.variables[var_name][:]=self.data_root.groups['soft_links'].variables[var_name][:]

            self.paths_sent_for_retrieval=[]
            for var_to_retrieve in self.retrievable_vars:
                self.retrieve_variable(output,var_to_retrieve)
        else:
            #Fixed variable. Do not retrieve, just copy:
            for var in self.retrievable_vars:
                output=netcdf_utils.replicate_and_copy_variable(self.data_root,output,var)
            output.sync()
        return

    def retrieve_variable(self,output,var_to_retrieve):
        #Replicate variable to output:
        output=netcdf_utils.replicate_netcdf_var(self.data_root,output,var_to_retrieve,chunksize=-1,zlib=True)

        if sum(self.time_restriction)==0:
            return

        #Get the requested dimensions:
        #self.get_dimensions_slicing()
        self.dimensions, self.unsort_dimensions=get_dimensions_slicing(self.data_root,var_to_retrieve,self.time_var)

        # Determine the paths_ids for soft links:
        self.paths_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][self.time_restriction,0][self.time_restriction_sort]
        self.indices_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][self.time_restriction,1][self.time_restriction_sort]

        #Convert paths_link to id in path dimension:
        #self.paths_link=np.array([list(self.path_id_list).index(path_id) for path_id in self.paths_link])
        #Use search sorted:
        self.paths_link=np.argsort(self.path_id_list)[np.searchsorted(self.path_id_list,self.paths_link,
                                                                        sorter=np.argsort(self.path_id_list))]

        #Sort the paths so that we query each only once:
        unique_path_list_id, self.sorting_paths=np.unique(self.paths_link,return_inverse=True)

        for unique_path_id, path_id in enumerate(unique_path_list_id):
            self.retrieve_path_to_variable(unique_path_id,path_id,output,var_to_retrieve)
        return

    def retrieve_path_to_variable(self,unique_path_id,path_id,output,var_to_retrieve):
        path_to_retrieve=self.path_list[path_id]

        #Next, we check if the file is available. If it is not we replace it
        #with another file with the same checksum, if there is one!
        file_type=self.file_type_list[list(self.path_list).index(path_to_retrieve)]
        remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,
                                                file_type,
                                                semaphores=self.semaphores,
                                                **self.remote_netcdf_kwargs)

        #See if the available path is available for download and find alternative:
        if self.retrieval_type=='download_files':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,remote_netcdf.downloadable_file_types,num_trials=2)
        elif self.retrieval_type=='download_opendap':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,remote_netcdf.remote_queryable_file_types,num_trials=2)
        elif self.retrieval_type=='load':
            path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,remote_netcdf.local_queryable_file_types,num_trials=2)

        if path_to_retrieve==None:
            #Do not retrieve!
            return

        #See if the available path is available for download and find alternative:
        if self.retrieval_type=='download_files' and not self.download_all_files:
            alt_path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,
                                                                remote_netcdf.remote_queryable_file_types+
                                                                remote_netcdf.local_queryable_file_types,num_trials=2)
        elif self.retrieval_type=='download_opendap' and not self.download_all_opendap:
            alt_path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,remote_netcdf.local_queryable_file_types,num_trials=2)
        else:
            alt_path_to_retrieve=None
        if alt_path_to_retrieve!=None:
            #Do not retrieve if a 'better' file type exists and is available
            return
            
        #Get the file_type, checksum and version of the file to retrieve:
        path_index=list(self.path_list).index(path_to_retrieve)
        file_type=self.file_type_list[path_index]
        version='v'+str(self.version_list[path_index])
        checksum=self.checksum_list[path_index]
        checksum_type=self.checksum_type_list[path_index]

        #Reverse pick time indices correponsing to the unique path_id:
        if file_type=='soft_links_container':
            #if the data is in the current file, the data lies in the corresponding time step:
            time_indices=np.arange(len(self.sorting_paths),dtype=int)[self.sorting_paths==unique_path_id]
        else:
            time_indices=self.indices_link[self.sorting_paths==unique_path_id]

        download_args=(0,path_to_retrieve,file_type,var_to_retrieve,self.tree)
        if self.retrieval_type=='download_files':
            if path_to_retrieve in self.paths_sent_for_retrieval:
                new_path=http_netcdf.destination_download_files(self.path_list[path_index],
                                                                     self.out_dir,
                                                                     var_to_retrieve,
                                                                     self.path_list[path_index],
                                                                     self.tree)
                new_file_type='local_file'
                self.add_path_to_soft_links(new_path,new_file_type,path_index,self.sorting_paths==unique_path_id,output.groups['soft_links'],var_to_retrieve)
                download_kwargs={'out_dir':self.out_dir,
                                 'version':version,
                                 'checksum':checksum,
                                 'checksum_type':checksum_type}
        else:
            #This is an important test that should be included in future releases:
            #with netCDF4.Dataset(path_to_retrieve.split('|')[0]) as data_test:
            #    data_date_axis=netcdf_utils.get_date_axis(data_test.variables['time'])[time_indices]
            #print(path_to_retrieve,self.date_axis[self.time_restriction][self.time_restriction_sort][self.sorting_paths==unique_path_id],data_date_axis)
            self.dimensions[self.time_var], self.unsort_dimensions[self.time_var] = indices_utils.prepare_indices(time_indices)

            if self.retrieval_type=='download_opendap':
                new_path='soft_links_container/'+os.path.basename(self.path_list[path_index])
                new_file_type='soft_links_container'
                self.add_path_to_soft_links(new_path,new_file_type,path_index,self.sorting_paths==unique_path_id,output.groups['soft_links'],var_to_retrieve)
            sort_table=np.arange(len(self.sorting_paths))[self.sorting_paths==unique_path_id]
            download_kwargs={'dimensions':self.dimensions,
                             'unsort_dimensions':self.unsort_dimensions,
                             'sort_table':sort_table
                             }

        #Keep a list of paths sent for retrieval:
        self.paths_sent_for_retrieval.append(path_to_retrieve)

        if self.retrieval_type!='load':
            data_node=remote_netcdf.get_data_node(path_to_retrieve,file_type)
            #Send to the download queue:
            self.queues.put_to_data_node(data_node,download_args+(download_kwargs,))
        else:
            #Load and simply assign:
            if file_type=='soft_links_container':
                max_request=2048
                retrieved_data=netcdf_utils.retrieve_container(self.data_root,
                                                                var_to_retrieve,
                                                                self.dimensions,
                                                                self.unsort_dimensions,
                                                                sort_table,max_request)
                result=(retrieved_data, sort_table, self.tree+[var_to_retrieve,])
            else:
                remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,file_type)
                result=remote_data.download(*download_args[3:],download_kwargs=download_kwargs)
            assign_leaf(output,*result)
            #output.sync()
        return 

    def add_path_to_soft_links(self,new_path,new_file_type,path_index,time_indices_to_replace,output,var_to_retrieve):
        if not new_path in output.variables['path'][:]:
            output.variables['path'][len(output.dimensions['path'])]=new_path
            output.variables['path_id'][-1]=hash(new_path)
            output.variables['file_type'][-1]=new_file_type
            output.variables['data_node'][-1]=remote_netcdf.get_data_node(new_path,output.variables['file_type'][-1])
            for path_desc in ['version']+file_unique_id_list:
                output.variables[path_desc][-1]=getattr(self,path_desc+'_list')[path_index]
        
        output.variables[var_to_retrieve][time_indices_to_replace,0]=output.variables['path_id'][-1]
        return output

    def open(self):
        self.tree=[]
        self.filepath='temp_file.pid'+str(os.getpid())
        self.output_root=netCDF4.Dataset(self.filepath,
                                      'w',format='NETCDF4',diskless=True,persist=False)
        return

    def __enter__(self):
        self.open()
        return self

    def assign(self,var_to_retrieve,requested_time_restriction):
        self.variables=dict()
        self.time_restriction=np.array(requested_time_restriction)
        self.time_restriction_sort=np.argsort(self.date_axis[self.time_restriction])
        self.retrieval_type='load'
        self.out_dir='.'
        self.paths_sent_for_retrieval=[]
    
        self.output_root.createGroup(var_to_retrieve)
        netcdf_utils.create_time_axis(self.data_root,self.output_root.groups[var_to_retrieve],self.time_axis[self.time_restriction][self.time_restriction_sort])
        self.retrieve_variable(self.output_root.groups[var_to_retrieve],var_to_retrieve)
        for var in self.output_root.groups[var_to_retrieve].variables.keys():
            self.variables[var]=self.output_root.groups[var_to_retrieve].variables[var]
        return

    def close(self):
        self.output_root.close()
        return

    def __exit__(self, *_):
        self.close()
        return

def add_previous(time_restriction):
    return np.logical_or(time_restriction,np.append(time_restriction[1:],False))

def add_next(time_restriction):
    return np.logical_or(time_restriction,np.insert(time_restriction[:-1],0,False))

def time_restriction_years(options,date_axis,time_restriction_any):
    if 'year' in dir(options) and options.year!=None:
        years_axis=np.array([date.year for date in date_axis])
        if 'min_year' in dir(options) and options.min_year!=None:
            #Important for piControl:
            time_restriction=np.logical_and(time_restriction_any, [True if year in options.year else False for year in years_axis-years_axis.min()+options.min_year])
        else:
            time_restriction=np.logical_and(time_restriction_any, [True if year in options.year else False for year in years_axis])
        return time_restriction
    else:
        return time_restriction_any

def time_restriction_months(options,date_axis,time_restriction_for_years):
    if 'month' in dir(options) and options.month!=None:
        months_axis=np.array([date.month for date in date_axis])
        #time_restriction=np.logical_and(time_restriction,months_axis==month)
        time_restriction=np.logical_and(time_restriction_for_years,[True if month in options.month else False for month in months_axis])
        #Check that months are continuous:
        if options.month==[item for item in options.month if (item % 12 +1 in options.month or item-2% 12+1 in options.month)]:
            time_restriction_copy=copy.copy(time_restriction)
            #Months are continuous further restrict time_restriction to preserve continuity:
            if time_restriction[0] and months_axis[0]-2 % 12 +1 in options.month:
                time_restriction[0]=False
            if time_restriction[-1] and months_axis[-1] % 12 +1 in options.month:
                time_restriction[-1]=False

            for id in range(len(time_restriction))[1:-1]:
                if time_restriction[id]:
                    if (( ((months_axis[id-1]-1)-(months_axis[id]-1)) % 12 ==11 or
                         months_axis[id-1] == months_axis[id] ) and
                        not time_restriction[id-1]):
                        time_restriction[id]=False

            for id in reversed(range(len(time_restriction))[1:-1]):
                if time_restriction[id]:
                    if (( ((months_axis[id+1]-1)-(months_axis[id]-1)) % 12 ==1 or
                         months_axis[id+1] == months_axis[id] ) and
                        not time_restriction[id+1]):
                        time_restriction[id]=False
            #If all values were eliminated, do not ensure continuity:
            if not np.any(time_restriction):
                time_restriction=time_restriction_copy
        return time_restriction
    else:
        return time_restriction_for_years

def time_restriction_days(options,date_axis,time_restriction_any):
    if 'day' in dir(options) and options.day!=None:
        days_axis=np.array([date.day for date in date_axis])
        time_restriction=np.logical_and(time_restriction_any,[True if day in options.day else False for day in days_axis])
        return time_restriction
    else:
        return time_restriction_any
                    
def time_restriction_hours(options,date_axis,time_restriction_any):
    if 'hour' in dir(options) and options.hour!=None:
        hours_axis=np.array([date.hour for date in date_axis])
        time_restriction=np.logical_and(time_restriction_any,[True if hour in options.hour else False for hour in hours_axis])
        return time_restriction
    else:
        return time_restriction_any
                    
def get_time_restriction(date_axis,options):
    time_restriction=np.ones(date_axis.shape,dtype=np.bool)

    time_restriction=time_restriction_years(options,date_axis,time_restriction)
    time_restriction=time_restriction_months(options,date_axis,time_restriction)
    time_restriction=time_restriction_days(options,date_axis,time_restriction)
    time_restriction=time_restriction_hours(options,date_axis,time_restriction)

    if ( ('previous' in dir(options) and options.previous>0) or
         ('next' in dir(options) and options.next>0) ):
        sorted_time_restriction=time_restriction[np.argsort(date_axis)]
        if 'previous' in dir(options) and options.previous>0:
            for prev_num in range(options.previous):
                sorted_time_restriction=add_previous(sorted_time_restriction)
        if 'next' in dir(options) and options.next>0:
            for next_num in range(options.next):
                sorted_time_restriction=add_next(sorted_time_restriction)
        time_restriction[np.argsort(date_axis)]=sorted_time_restriction
    return time_restriction

def get_dimensions_slicing(dataset,var,time_var):
    #Set the dimensions:
    dimensions=dict()
    unsort_dimensions=dict()
    for dim in dataset.variables[var].dimensions:
        if dim != time_var:
            if dim in dataset.variables.keys():
                dimensions[dim] = dataset.variables[dim][:]
            else:
                dimensions[dim] = np.arange(len(dataset.dimensions[dim]))
            unsort_dimensions[dim] = None
    return dimensions, unsort_dimensions

def assign_leaf(output,val,sort_table,tree):
    output.variables[tree[-1]][sort_table,...]=val
    return
