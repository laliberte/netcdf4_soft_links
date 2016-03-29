#External:
import numpy as np
import netCDF4
import os
import copy

#Internal:
import netcdf_utils
import remote_netcdf
import indices_utils
import retrieval_utils

queryable_file_types=['OPENDAP','local_file']
remote_queryable_file_types=['OPENDAP']
raw_file_types=['FTPServer','HTTPServer','GridFTP']

file_unique_id_list=['checksum_type','checksum','tracking_id']

class read_netCDF_pointers:
    def __init__(self,data_root,options=None,queues=dict(),semaphores=dict()):
        self.data_root=data_root
        #Queues and semaphores for safe asynchronous retrieval:
        self.queues=queues
        self.semaphores=semaphores

        #Include slicing of data_nodes:
        for slice_id in ['data_node','Xdata_node']:
            if slice_id in dir(options):
                setattr(self,slice_id,getattr(options,slice_id))
        for opt in ['username','password']:
            if opt in dir(options):
                setattr(self,opt,getattr(options,opt))

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
            self.time_axis, self.time_restriction=retrieve_time_axis(self.data_root,options)
        else:
            self.time_axis, self.time_restriction=None,None
        return

    def replicate(self,output,hdf5=None,check_empty=False):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(output,self.data_root)
        #replicate and copy variables:
        for var_name in self.data_root.variables.keys():
            netcdf_utils.replicate_and_copy_variable(output,self.data_root,var_name,hdf5=hdf5,check_empty=check_empty)
        if 'soft_links' in self.data_root.groups.keys():
            output_grp=netcdf_utils.replicate_group(output,self.data_root,'soft_links')
            netcdf_utils.replicate_netcdf_file(output_grp,self.data_root.groups['soft_links'])
            for var_name in self.data_root.groups['soft_links'].variables.keys():
                if hdf5!=None:
                    netcdf_utils.replicate_and_copy_variable(output_grp,self.data_root.groups['soft_links'],var_name,hdf5=hdf5['soft_links'],check_empty=check_empty)
                else:
                    netcdf_utils.replicate_and_copy_variable(output_grp,self.data_root.groups['soft_links'],var_name,check_empty=check_empty)
        return

    def retrieve_without_time(self,retrieval_function_name,output):
        #This function simply retrieves all the files:
        file_path=output
        for path_to_retrieve in self.path_list:
            path_index=list(self.path_list).index(path_to_retrieve)
            file_type=self.file_type_list[path_index]
            version='v'+str(self.version_list[path_index])

            #Get the file tree:
            args = ({'path':'|'.join([path_to_retrieve,] +
                               [ getattr(self,file_unique_id+'_list')[path_index] for file_unique_id in file_unique_id_list]),
                    'var':self.tree[-1],
                    'file_path':file_path,
                    'version':version,
                    'file_type':file_type,
                    'username':self.username,
                    'user_pass':self.password},
                    copy.deepcopy(self.tree))

            #Retrieve only if it is from the requested data node:
            data_node=remote_netcdf.get_data_node(path_to_retrieve,file_type)
            if is_level_name_included_and_not_excluded('data_node',self,data_node):
                if data_node in self.queues.keys():
                    print 'Recovering '+'/'.join(self.tree)
                    self.queues[data_node].put((getattr(retrieval_utils,retrieval_function_name),)+copy.deepcopy(args))
        return

    def retrieve(self,output,retrieval_function_name):
        #Define tree:
        self.tree=output.path.split('/')[1:]
        self.retrieval_function_name=retrieval_function_name

        if self.retrieval_function_name=='retrieve_path_data':
            self.acceptable_file_types=queryable_file_types
        elif self.retrieval_function_name=='retrieve_path':
            self.acceptable_file_types=raw_file_types

        if self.time_var!=None:
            #Record to output if output is a netCDF4 Dataset:
            if not self.time_var in output.dimensions.keys():
                netcdf_utils.create_time_axis(output,self.data_root,self.time_axis[self.time_restriction])

            #Replicate all the other variables:
            for var in set(self.data_root.variables.keys()).difference(self.retrievable_vars):
                if not var in output.variables.keys():
                    output=netcdf_utils.replicate_and_copy_variable(output,self.data_root,var)

            self.retrieval_queue_list=[]
            for var_to_retrieve in self.retrievable_vars:
                self.retrieve_variable(output,var_to_retrieve)

            for arg in self.retrieval_queue_list:
                if is_level_name_included_and_not_excluded('data_node',self,arg[0]['data_node']):
                    if arg[0]['data_node'] in self.queues.keys():
                        self.queues[arg[0]['data_node']].put((getattr(retrieval_utils,self.retrieval_function_name),)+arg)
                    elif (self.retrieval_function_name=='retrieve_path_data'):
                        netcdf_utils.assign_leaf(output,*getattr(retrieval_utils,self.retrieval_function_name)(arg[0],arg[1]))
        else:
            #Fixed variable. Do not retrieve, just copy:
            for var in self.retrievable_vars:
                output=netcdf_utils.replicate_and_copy_variable(output,self.data_root,var)
            output.sync()

            ##Downloading before a complete validate has been performed:
            #self.retrieve_without_time(retrieval_function_name,output)
        return

    def retrieve_variable(self,output,var_to_retrieve):
        self.var_to_retrieve=var_to_retrieve

        #Replicate variable to output:
        output=netcdf_utils.replicate_netcdf_var(output,self.data_root,self.var_to_retrieve,chunksize=-1,zlib=True)
        if not 'soft_links' in self.data_root.groups.keys():
            #Variable is stored here and simply retrieve it:
            output.variables[self.var_to_retrieve][:]=self.data_root.variables[self.var_to_retrieve][self.time_restriction]
            return

        #Get the requested dimensions:
        self.get_dimensions_slicing()

        # Determine the paths_ids for soft links:
        self.paths_link=self.data_root.groups['soft_links'].variables[self.var_to_retrieve][self.time_restriction,0]
        self.indices_link=self.data_root.groups['soft_links'].variables[self.var_to_retrieve][self.time_restriction,1]

        #Convert paths_link to id in path dimension:
        self.paths_link=np.array([list(self.path_id_list).index(path_id) for path_id in self.paths_link])

        #Sort the paths so that we query each only once:
        self.unique_path_list_id, self.sorting_paths=np.unique(self.paths_link,return_inverse=True)

        for unique_path_id, path_id in enumerate(self.unique_path_list_id):
            self.retrieve_path_to_variable(unique_path_id,path_id)
        return

    def retrieve_path_to_variable(self,unique_path_id,path_id):
        path_to_retrieve=self.path_list[path_id]

        #Next, we check if the file is available. If it is not we replace it
        #with another file with the same checksum, if there is one!
        file_type=self.file_type_list[list(self.path_list).index(path_to_retrieve)]
        remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,file_type,self.semaphores)

        #if not file_type in ['FTPServer']:
        self.path_to_retrieve=remote_data.check_if_available_and_find_alternative(self.path_list,self.file_type_list,self.checksum_list,self.acceptable_file_types)
        if self.path_to_retrieve==None:
            #Do not retrieve!
            return

        #Define file path:
        self.file_path=None

        #Get the file_type, checksum and version of the file to retrieve:
        self.path_index=list(self.path_list).index(self.path_to_retrieve)
        self.file_type=self.file_type_list[self.path_index]
        self.version='v'+str(self.version_list[self.path_index])

        #Append the checksum:
        self.path_to_retrieve='|'.join([self.path_to_retrieve,] +
                           [ getattr(self,file_unique_id+'_list')[self.path_index] for file_unique_id in file_unique_id_list])

        self.data_node=remote_netcdf.get_data_node(self.path_to_retrieve,self.file_type)

        #Reverse pick time indices correponsing to the unique path_id:
        self.time_indices=self.indices_link[self.sorting_paths==unique_path_id]

        max_request=450 #maximum request in Mb
        max_time_steps=max(int(np.floor(max_request*1024*1024/(32*np.prod(self.dims_length)))),1)
        #Maximum number of time step per request:
        if self.retrieval_function_name=='retrieve_path':
            num_time_chunk=1
        else:
            num_time_chunk=int(np.ceil(len(self.time_indices)/float(max_time_steps)))
        for time_chunk in range(num_time_chunk):
            time_slice=slice(time_chunk*max_time_steps,(time_chunk+1)*max_time_steps,1)
            self.retrieve_time_chunk(time_slice,unique_path_id)

    def retrieve_time_chunk(self,time_slice,unique_path_id):
        self.dimensions[self.time_var], self.unsort_dimensions[self.time_var] = indices_utils.prepare_indices(self.time_indices[time_slice])
        
        #Get the file tree:
        self.retrieval_queue_list.append(
                copy.deepcopy( (
               {'path':self.path_to_retrieve,
                'var':self.var_to_retrieve,
                'indices':self.dimensions,
                'unsort_indices':self.unsort_dimensions,
                'sort_table':np.arange(len(self.sorting_paths))[self.sorting_paths==unique_path_id][time_slice],
                'file_path':self.file_path,
                'version':self.version,
                'file_type':self.file_type,
                'data_node':self.data_node,
                'username':self.username,
                'user_pass':self.password},
                self.tree) ) )

        return 

    def get_dimensions_slicing(self):
        #Set the dimensions:
        self.dimensions=dict()
        self.unsort_dimensions=dict()
        self.dims_length=[]
        for dim in self.data_root.variables[self.var_to_retrieve].dimensions:
            if dim != self.time_var:
                if dim in self.data_root.variables.keys():
                    self.dimensions[dim] = self.data_root.variables[dim][:]
                else:
                    self.dimensions[dim] = np.arange(len(self.data_root.dimensions[dim]))
                self.unsort_dimensions[dim] = None
                self.dims_length.append(len(self.dimensions[dim]))
        return 

    def open(self):
        self.initialize_retrieval()
        self.tree=[]
        self.output_root=netCDF4.Dataset('temp_file.pid'+str(os.getpid()),
                                      'w',format='NETCDF4',diskless=True,persist=False)
        return

    def assign(self,var_to_retrieve,requested_time_restriction):
        self.variables=dict()
        self.time_restriction=np.array(requested_time_restriction)
        self.retrieval_function_name='retrieve_path_data'

        self.output_root.createGroup(var_to_retrieve)
        netcdf_utils.create_time_axis(self.output_root.groups[var_to_retrieve],self.data_root,self.time_axis[self.time_restriction])
        self.retrieve_variable(var_to_retrieve,self.output_root.groups[var_to_retrieve])
        for var in self.output_root.groups[var_to_retrieve].variables.keys():
            self.variables[var]=self.output_root.groups[var_to_retrieve].variables[var]
        return

    def close(self):
        self.output_root.close()
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
                    #print date_axis[id], months_axis[id-1],months_axis[id], months_axis[id]-2 % 12 +1, time_restriction[id-1]
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

def retrieve_time_axis(data,options):
    time_var=netcdf_utils.find_time_var(data)
    time_axis=data.variables[time_var][:]
    time_restriction=np.ones(time_axis.shape,dtype=np.bool)

    date_axis=netcdf_utils.get_date_axis(data.variables[time_var])
    time_restriction=time_restriction_years(options,date_axis,time_restriction)
    time_restriction=time_restriction_months(options,date_axis,time_restriction)
    time_restriction=time_restriction_days(options,date_axis,time_restriction)
    time_restriction=time_restriction_hours(options,date_axis,time_restriction)
    if 'previous' in dir(options) and options.previous>0:
        for prev_num in range(options.previous):
            time_restriction=add_previous(time_restriction)
    if 'next' in dir(options) and options.next>0:
        for next_num in range(options.next):
            time_restriction=add_next(time_restriction)
    return time_axis,time_restriction

