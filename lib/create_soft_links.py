#External:
import numpy as np
import tempfile
import netCDF4
import copy
import os

#Internal:
import retrieval_utils
import netcdf_utils
import remote_netcdf


queryable_file_types=['OPENDAP','local_file','soft_link_container']
unique_file_id_list=['checksum_type','checksum','tracking_id']

class create_netCDF_pointers:
    def __init__(self,paths_list,time_frequency,years,months,file_type_list,data_node_list,semaphores=dict(),record_other_vars=True):
        self.file_type_list=file_type_list
        self.data_node_list=data_node_list
        self.semaphores=semaphores
        self.paths_list=paths_list

        self.sorts_list=['version','file_type_id','data_node_id','path_id']
        self.id_list=['data_node','file_type','path']+unique_file_id_list

        self.time_frequency=time_frequency
        self.is_instant=False
        self.record_other_vars=record_other_vars

        self.months=months
        self.years=years

        self.paths_ordering=self.order_paths_by_preference()
        return

    def record_paths(self,output,var,username=None,user_pass=None):
        self.create(output)
        return

    def record_meta_data(self,output,var,username=None,user_pass=None):
        if self.time_frequency in ['fx','clim']:
            if isinstance(var,list):
                for sub_var in var:
                    self.record_fx(output,sub_var,username=username,user_pass=user_pass)
            else:
                self.record_fx(output,var,username=username,user_pass=user_pass)
        else:
            self.calendar=self.obtain_unique_calendar()
            #Retrieve time and meta:
            self.create_variable(output,var)
            #Put version:
            output.setncattr(str('netcdf_soft_links_version'),str('1.3'))
        return

    def record_fx(self,output,var,username=None,user_pass=None):
        #Find the most recent version:
        most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in self.paths_list]))
        usable_paths_list=[ item for item in self.paths_list if item['version']==most_recent_version]

        queryable_paths_list=[item for item in usable_paths_list if item['file_type'] in queryable_file_types]
        if len(queryable_paths_list)==0:
            temp_file_handle, temp_file_name=tempfile.mkstemp()
        try:
            if len(queryable_paths_list)==0:
                path=usable_paths_list[0]
                #Download the file to temp
                retrieval_utils.download_secure(path['path'].split('|')[0],
                                temp_file_name,
                                path['file_type'],
                                username=username,user_pass=user_pass)
                remote_data=remote_netcdf.remote_netCDF(temp_file_name,path['file_type'],self.semaphores)
            else:
                #Check if data in available:
                path = queryable_paths_list[0]

                remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],self.semaphores)
                alt_path_name=remote_data.check_if_available_and_find_alternative([item['path'].split('|')[0] for item in queryable_paths_list],
                                                                            [item['file_type'] for item in queryable_paths_list],
                                                                         [item['path'].split('|')[unique_file_id_list.index('checksum')+1] for item in queryable_paths_list])

                #Use aternative path:
                path=queryable_paths_list[[item['path'].split('|')[0] for item in queryable_paths_list].index(alt_path_name)]
                remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],self.semaphores)
            output=remote_data.retrieve_variables(output,zlib=True)
            output.sync()

            for att in path.keys():
                if att!='path':      
                    output.setncattr(att,path[att])
            output.setncattr('path',path['path'].split('|')[0])
            for unique_file_id in unique_file_id_list:
                output.setncattr(unique_file_id,path['path'].split('|')[unique_file_id_list.index(unique_file_id)+1])
            output.sync()
        finally:
            pass
            if len(queryable_paths_list)==0:
                os.remove(temp_file_name)

        #Create soft links
        self.create(output)
        output.groups['soft_links'].createVariable(var,np.float32,(),zlib=True)
        return

    def create(self,output):
        if not 'soft_links' in output.groups.keys():
            output_grp=output.createGroup('soft_links')
        else:
            output_grp=output.groups['soft_links']

        #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
        #output_grp.createDimension('path',len(self.paths_ordering))
        output_grp.createDimension('path',None)
        for id in ['version','path_id']:
            output_grp.createVariable(id,np.int64,('path',))[:]=self.paths_ordering[id]
        for id in self.id_list:
            temp=output_grp.createVariable(id,str,('path',))
            for file_id, file in enumerate(self.paths_ordering['path']):
                temp[file_id]=str(self.paths_ordering[id][file_id])
        return 

    def order_paths_by_preference(self):
        #FIND ORDERING:
        paths_desc=[]
        for id in self.sorts_list:
            paths_desc.append((id,np.int64))
        for id in self.id_list:
            paths_desc.append((id,'a255'))
        paths_ordering=np.empty((len(self.paths_list),), dtype=paths_desc)
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['path'][file_id]=file['path'].split('|')[0]
            #Convert path name to 'unique' integer using hash.
            #The integer will not really be unique but collisions
            #should be extremely rare for similar strings with only small variations.
            paths_ordering['path_id'][file_id]=hash(
                                                    paths_ordering['path'][file_id]
                                                        )
            for unique_file_id in unique_file_id_list:
                paths_ordering[unique_file_id][file_id]=file['path'].split('|')[unique_file_id_list.index(unique_file_id)+1]
            paths_ordering['version'][file_id]=np.long(file['version'][1:])

            paths_ordering['file_type'][file_id]=file['file_type']
            paths_ordering['data_node'][file_id]=remote_netcdf.get_data_node(file['path'],paths_ordering['file_type'][file_id])

        #Sort paths from most desired to least desired:
        #First order desiredness for least to most:
        data_node_order=copy.copy(self.data_node_list)[::-1]#list(np.unique(paths_ordering['data_node']))
        file_type_order=copy.copy(self.file_type_list)[::-1]#list(np.unique(paths_ordering['file_type']))
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['data_node_id'][file_id]=data_node_order.index(paths_ordering['data_node'][file_id])
            paths_ordering['file_type_id'][file_id]=file_type_order.index(paths_ordering['file_type'][file_id])
        #'version' is implicitly from least to most

        #sort and reverse order to get from most to least:
        return np.sort(paths_ordering,order=self.sorts_list)[::-1]

    def _recover_time(self,path):
        file_type=path['file_type']
        path_name=str(path['path']).split('|')[0]
        remote_data=remote_netcdf.remote_netCDF(path_name,file_type,self.semaphores)
        time_axis=remote_data.get_time(time_frequency=self.time_frequency,
                                        is_instant=self.is_instant,
                                        calendar=self.calendar)
        table_desc=[
                   ('paths','a255'),
                   ('file_type','a255'),
                   ('indices','int64')
                   ] + [(unique_file_id,'a255') for unique_file_id in unique_file_id_list]
        table=np.empty(time_axis.shape, dtype=table_desc)
        if len(time_axis)>0:
            table['paths']=np.array([str(path_name) for item in time_axis])
            table['file_type']=np.array([str(file_type) for item in time_axis])
            table['indices']=range(0,len(time_axis))
            for unique_file_id in unique_file_id_list:
                table[unique_file_id]=np.array([str(path[unique_file_id]) for item in time_axis])
        return time_axis,table
    
    def obtain_time_axis(self):
        #Retrieve time axes from queryable file types or reconstruct time axes from time stamp
        #from non-queryable file types.
        self.time_axis, self.table= map(np.concatenate,
                        zip(*map(self._recover_time,np.nditer(self.paths_ordering))))
        if len(self.time_axis)>0:
            self.units='days since '+str(np.sort(self.time_axis)[0])
        return

    def _recover_calendar(self,path):
        file_type=path['file_type']
        path_name=str(path['path']).split('|')[0]
        remote_data=remote_netcdf.remote_netCDF(path_name,file_type,self.semaphores)
        calendar=remote_data.get_calendar()
        return calendar, file_type 

    def obtain_unique_calendar(self):
        calendar_list,file_type_list=zip(*map(self._recover_calendar,np.nditer(self.paths_ordering)))
        #Find the calendars found from queryable file types:
        calendars = set([item[0] for item in zip(calendar_list,file_type_list) if item[1] in queryable_file_types])
        if len(calendars)==1:
            return calendars.pop()
        return calendar_list[0]

    def reduce_paths_ordering(self):
        #CREATE LOOK-UP TABLE:
        self.paths_indices=np.empty(self.time_axis.shape,dtype=np.int64)
        self.time_indices=np.empty(self.time_axis.shape,dtype=np.int64)

        paths_list=[path for path in self.paths_ordering['path'] ]
        paths_id_list=[path_id for path_id in self.paths_ordering['path_id'] ]
        #for path_id, path in zip(paths_id_list,paths_list):
        for path_id, path in enumerate(paths_list):
            #find in self.table the path and assign path_id to it:
            self.paths_indices[path==self.table['paths']]=path_id

        #Remove paths that are not necessary over the requested time range:
        #First, list the paths_id used:
        useful_paths_id_list_unique=list(np.unique([paths_id_list[np.min(self.paths_indices[time==self.time_axis])]  for time_id, time in enumerate(self.time_axis_unique)]))
        #Second, list the path_names corresponding to these paths_id:
        useful_paths_id_list=[useful_path_id for useful_path_id in 
                                [path_id for path_id, path in zip(paths_id_list,paths_list)
                                        if path_id in useful_paths_id_list_unique] ]
        useful_file_name_list=[useful_path_id.split('/')[-1] for useful_path_id in 
                                [path for path_id, path in zip(paths_id_list,paths_list)
                                        if path_id in useful_paths_id_list_unique] ]

        #Find the indices to keep:
        useful_file_id_list=[file_id for file_id, file in enumerate(self.paths_ordering)
                                if self.paths_ordering['path_id'][file_id] in useful_paths_id_list]
                                
        #Finally, check if some equivalent indices are worth keeping:
        for file_id, file in enumerate(self.paths_ordering):
            if not self.paths_ordering['path_id'][file_id] in useful_paths_id_list:
                #This file was not kept but it might be the same data, in which
                #case we would like to keep it.
                #Find the file name (remove path):
                file_name=self.paths_ordering['path'][file_id].split('/')[-1]
                if file_name in useful_file_name_list:
                    #If the file name is useful, find its path_id: 
                    equivalent_path_id=useful_paths_id_list[useful_file_name_list.index(file_name)]
                        
                    #Use this to find its file_id:
                    equivalent_file_id=list(self.paths_ordering['path_id']).index(equivalent_path_id)
                    #Then check if the checksum are the same. If yes, keep the file!
                    if self.paths_ordering['checksum'][file_id]==self.paths_ordering['checksum'][equivalent_file_id]:
                        useful_file_id_list.append(file_id)
                
        #Sort paths_ordering:
        if len(useful_file_id_list)>0:
            self.paths_ordering=self.paths_ordering[np.sort(useful_file_id_list)]
            
        #The last lines were commented to allow for collision-free (up to 32-bits hashing
        #algorithm) indexing.

        #Finally, set the path_id field to be following the indices in paths_ordering:
        #self.paths_ordering['path_id']=range(len(self.paths_ordering))

        #Recompute the indices to paths:
        #paths_list=[path for path in self.paths_ordering['path'] ]
        #for path_id, path in enumerate(paths_list):
        #    self.paths_indices[path.replace('fileServer','dodsC')==self.table['paths']]=path_id
        return

    def unique_time_axis(self):
        time_axis = netCDF4.date2num(self.time_axis,self.units,calendar=self.calendar)
        time_axis_unique = np.unique(time_axis)

        time_axis_unique_date=netCDF4.num2date(time_axis_unique,self.units,calendar=self.calendar)

        #Include a filter on years and months: 
        time_desc={}
        if self.years!=None:
            if self.years[0]<10:
                #This is important for piControl
                temp_years=list(np.array(years)+np.min([date.year for date in time_axis_unique_date]))
                #min_year=np.min([date.year for date in time_axis_unique_date])
            else:
                temp_years=self.years
            if self.months!=None:
                valid_times=np.array([True if (date.year in temp_years and 
                                         date.month in self.months) else False for date in  time_axis_unique_date])
            else:
                valid_times=np.array([True if date.year in temp_years else False for date in  time_axis_unique_date])
        else:
            if self.months!=None:
                valid_times=np.array([True if date.month in self.months else False for date in  time_axis_unique_date])
            else:
                valid_times=np.array([True for date in  time_axis_unique_date])
            
        self.time_axis_unique=time_axis_unique[valid_times]
        self.time_axis_unique_date=time_axis_unique_date[valid_times]
        self.time_axis=time_axis
        return

    def create_variable(self,output,var):
        #Recover time axis for all files:
        self.obtain_time_axis()

        if len(self.table['paths'])>0:
            #Convert time axis to numbers and find the unique time axis:
            self.unique_time_axis()

            self.reduce_paths_ordering()

            #Load data
            queryable_file_types_available=list(set(self.table['file_type']).intersection(queryable_file_types))
            if len(queryable_file_types_available)>0:
                #Open the first file and use its metadata to populate container file:
                first_id=list(self.table['file_type']).index(queryable_file_types_available[0])
                remote_data=remote_netcdf.remote_netCDF(self.table['paths'][first_id],self.table['file_type'][first_id],self.semaphores)
                try:
                    remote_data.open_with_error()
                    time_dim=netcdf_utils.find_time_dim(remote_data.Dataset)
                    netcdf_utils.replicate_netcdf_file(output,remote_data.Dataset)
                finally:
                    remote_data.close()
            else:
                remote_data=remote_netcdf.remote_netCDF(self.table['paths'][0],self.table['file_type'][0],self.semaphores)
                time_dim='time'

            #Create time axis in ouptut:
            netcdf_utils.create_time_axis_date(output,self.time_axis_unique_date,self.units,self.calendar,time_dim=time_dim)


            self.create(output)
            try:
                remote_data.open_with_error()
                if isinstance(var,list):
                    for sub_var in var:
                        self.record_indices(output,remote_data.Dataset,sub_var,time_dim)
                else:
                    self.record_indices(output,remote_data.Dataset,var,time_dim)
            finally:
                remote_data.close()

            output.sync()
        return

    def record_indices(self,output,data,var,time_dim):
        if data!=None:
            #Create descriptive vars:
            for other_var in data.variables.keys():
                if ( (not time_dim in data.variables[other_var].dimensions) and 
                     (not other_var in output.variables.keys())):
                    netcdf_utils.replicate_and_copy_variable(output,data,other_var)

        #CREATE LOOK-UP TABLE:
        output_grp=output.groups['soft_links']
        indices_dim='indices'
        if not indices_dim in output_grp.dimensions:
            output_grp.createDimension(indices_dim,2)
        if not indices_dim in output_grp.variables.keys():
            output_grp.createVariable(indices_dim,np.str,(indices_dim,))
        indices=output_grp.variables[indices_dim]
        indices[0]='path'
        indices[1]=time_dim

        if var in data.variables.keys():
            #Create main variable if it is in data:
            if data!=None:
                netcdf_utils.replicate_netcdf_var(output,data,var,chunksize=-1,zlib=True)
            else:
                output.createVariable(var,np.float32,(time_dim,),zlib=True)

            #var_out = output_grp.createVariable(var,np.int64,(time_dim,indices_dim),zlib=False,fill_value=np.iinfo(np.int64).max)
            var_out = output_grp.createVariable(var,np.int64,(time_dim,indices_dim),zlib=False)
            #Create soft links:
            paths_id_list=[path_id for path_id in self.paths_ordering['path_id'] ]

            for time_id, time in enumerate(self.time_axis_unique):
                path_index_to_use=np.min(self.paths_indices[time==self.time_axis])
                var_out[time_id,0]=paths_id_list[path_index_to_use]
                var_out[time_id,1]=self.table[indices_dim][np.logical_and(self.paths_indices==path_index_to_use,time==self.time_axis)][0]

        if data!=None:
            #Create support variables:
            for other_var in data.variables.keys():
                if ( (time_dim in data.variables[other_var].dimensions) and (other_var!=var) and
                     (not other_var in output.variables.keys()) and
                     self.record_other_vars):
                    netcdf_utils.replicate_netcdf_var(output,data,other_var,chunksize=-1,zlib=True)
                    #var_out = output_grp.createVariable(other_var,np.int64,(time_dim,indices_dim),zlib=False,fill_value=np.iinfo(np.int64).max)
                    var_out = output_grp.createVariable(other_var,np.int64,(time_dim,indices_dim),zlib=False)
                    #Create soft links:
                    for time_id, time in enumerate(self.time_axis_unique):
                        #var_out[time_id,0]=np.min(self.paths_indices[time==self.time_axis])
                        #var_out[time_id,1]=self.table[indices_dim][np.logical_and(self.paths_indices==var_out[time_id,0],time==self.time_axis)][0]
                        path_index_to_use=np.min(self.paths_indices[time==self.time_axis])
                        var_out[time_id,0]=paths_id_list[path_index_to_use]
                        var_out[time_id,1]=self.table[indices_dim][np.logical_and(self.paths_indices==path_index_to_use,time==self.time_axis)][0]
        return

