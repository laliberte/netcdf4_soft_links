#External:
import numpy as np
import math
import time
import netCDF4
import datetime
import copy
from collections import OrderedDict

#Internal:
import indices_utils

def get_year_axis(path_name):
    try:
        #print 'Loading file... ',
        #print path_name
        data=netCDF4.Dataset(path_name)
        dimensions_list=data.dimensions.keys()
        time_dim=find_time_dim(data)
        if time_dim not in dimensions_list:
            raise Error('time is missing from variable')
        date_axis = get_date_axis(data.variables[time_dim])
        #print ' Done!'
        data.close()
        year_axis=np.array([date.year for date in date_axis])
        month_axis=np.array([date.month for date in date_axis])
    except:
        return None, None

    return year_axis, month_axis

def get_date_axis(time_var):
    units=time_var.units
    if 'calendar' in dir(time_var):
        calendar=time_var.calendar
    else:
        calendar=None
    return get_date_axis_units(time_var[:],units,calendar)

def get_date_axis_units(time_axis,units,calendar):
    if units=='day as %Y%m%d.%f':
        date_axis=get_date_axis_absolute(time_axis)
    else:
        date_axis=get_date_axis_relative(time_axis,units,calendar)
    return date_axis

def get_date_axis_relative(time_axis,units,calendar):
    if calendar!=None:
        try:
            date_axis = netCDF4.num2date(time_axis,units=units,calendar=calendar)
        except ValueError:
            if (
                (units=='days since 0-01-01 00:00:00' and
                calendar=='365_day') or
                (units=='days since 0-1-1 00:00:00' and
                calendar=='365_day') 
                ):
                date_axis = netCDF4.num2date(time_axis-365.0,units='days since 1-01-01 00:00:00',calendar=calendar)
            else:
                raise
    else:
        date_axis = netCDF4.num2date(time_axis,units=units)
    return date_axis

def get_date_axis_absolute(time_axis):
    return map(convert_to_date_absolute,time_axis)

def get_time(data):
    time_dim=find_time_dim(data)
    time_axis, attributes=retrieve_dimension(data,time_dim)
    date_axis=create_date_axis_from_time_axis(time_axis,attributes)
    return date_axis

def get_time_axis_relative(date_axis,units,calendar):
    if calendar!=None:
        try:
            time_axis = netCDF4.date2num(date_axis,units=units,calendar=calendar)
        except ValueError:
            if (
                (units=='days since 0-01-01 00:00:00' and
                calendar=='365_day') or
                (units=='days since 0-1-1 00:00:00' and
                calendar=='365_day') 
                ):
                time_axis = netCDF4.date2num(date_axis,units='days since 1-01-01 00:00:00',calendar=calendar)+365.0
            else:
                raise
    else:
        time_axis = netCDF4.date2num(date_axis,units=units)
    return time_axis

def convert_to_date_absolute(absolute_time):
    year=int(math.floor(absolute_time/1e4))
    remainder=absolute_time-year*1e4
    month=int(math.floor(remainder/1e2))
    remainder-=month*1e2
    day=int(math.floor(remainder))
    remainder-=day
    remainder*=24.0
    hour=int(math.floor(remainder))
    remainder-=hour
    remainder*=60.0
    minute=int(math.floor(remainder))
    remainder-=minute
    remainder*=60.0
    seconds=int(math.floor(remainder))
    return datetime.datetime(year,month,day,hour,minute,seconds)

#def replicate_full_netcdf_recursive(output,data,options=None):
def replicate_full_netcdf_recursive(output,data,hdf5=None,check_empty=False):
    for var_name in data.variables.keys():
        replicate_and_copy_variable(output,data,var_name,hdf5=hdf5,check_empty=check_empty)
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            output_grp=replicate_group(output,data,group)
            if hdf5!=None:
                replicate_full_netcdf_recursive(output_grp,data.groups[group],hdf5=hdf5[group],check_empty=check_empty)
            else:
                replicate_full_netcdf_recursive(output_grp,data.groups[group],check_empty=check_empty)
    return

def dimension_compatibility(output,data,dim):
    if (dim in output.dimensions.keys()
        and len(output.dimensions[dim])!=len(data.dimensions[dim])):
        #Dimensions mismatch, return without writing anything
        return False
    elif ( (dim in data.variables.keys() and
          dim in output.variables.keys()) and
          ( len(output.variables[dim])!=len(data.variables[dim]) or 
           (data.variables[dim][:]!=data.variables[dim][:]).any())):
        #Dimensions variables mismatch, return without writing anything
        return False
    else:
        return True

def check_dimensions_compatibility(output,data,var_name,exclude_unlimited=False):
    for dim in data.variables[var_name].dimensions:
        #The dimensions might be in the parent group:
        if not dim in data.dimensions.keys():
            data_parent=data.parent
        else:
            data_parent=data
        if not dim in output.dimensions.keys():
            output_parent=output.parent
        else:
            output_parent=output
        if not data_parent.dimensions[dim].isunlimited() or not exclude_unlimited:
            if not dimension_compatibility(output_parent,data_parent,dim):
                return False
    return True

def append_record(output,data):
    record_dimensions=dict()
    for dim in data.dimensions.keys():
        if ( data.dimensions[dim].isunlimited()
             and dim in data.variables.keys()
             and dim in output.dimensions.keys()
             and dim in output.variables.keys()):
             append_slice=slice(len(output.dimensions[dim]),len(output.dimensions[dim])+
                                                          len(data.dimensions[dim]),1)
             ensure_compatible_time_units(output,data,dim)
             temp=data.variables[dim][:]
             output.variables[dim][append_slice]=temp
             record_dimensions[dim]={'append_slice':append_slice}
    return record_dimensions

def ensure_compatible_time_units(output,data,dim):
    for time_desc in ['units','calendar']:
        if ( (     time_desc in output.variables[dim].ncattrs()
               and time_desc in data.variables[dim].ncattrs()) and
              (output.variables[dim].getncattr(time_desc)!=data.variables[dim].getncattr(time_desc))):
            raise 'time units and calendar must be the same when appending soft links'
    return

def append_and_copy_variable(output,data,var_name,record_dimensions,datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None,hdf5=None,check_empty=False):
    if len(set(record_dimensions.keys()).intersection(data.variables[var_name].dimensions))==0:
        #Variable does not contain a record dimension, return
        return output
   
    variable_size=min(data.variables[var_name].shape)
    storage_size=variable_size
    #Use the hdf5 library to find the real size of the stored array:
    if hdf5!=None:
        variable_size=hdf5[var_name].size
        storage_size=hdf5[var_name].id.get_storage_size()

    if variable_size>0 and storage_size>0:
        max_request=450.0 #maximum request in Mb
        #max_request=9000.0 #maximum request in Mb
        max_time_steps=max(
                        int(np.floor(max_request*1024*1024/(32*np.prod(data.variables[var_name].shape[1:])))),
                        1)

        num_time_chunk=int(np.ceil(data.variables[var_name].shape[0]/float(max_time_steps)))
        for time_chunk in range(num_time_chunk):
            time_slice=slice(time_chunk*max_time_steps,
                             min((time_chunk+1)*max_time_steps,data.variables[var_name].shape[0])
                             ,1)
            output=append_data_time_slice(data,output,var_name,time_slice,record_dimensions,check_empty)
    return output

def append_data_time_slice(data,output,var_name,time_slice,record_dimensions,check_empty):
    #Create a setitem tuple
    setitem_list=[ slice(0,len(data.dimensions[dim]),1) if not dim in record_dimensions.keys()
                                                               else record_dimensions[dim]['append_slice']
                                                              for dim in data.variables[var_name].dimensions]
    #Pick a time_slice along the first dimension:
    setitem_list[0]=indices_utils.slice_a_slice(setitem_list[0],time_slice)
    temp=data.variables[var_name][time_slice,...]
    #Assign only if not masked everywhere:
    if not 'mask' in dir(temp) or not check_empty:
        output.variables[var_name].__setitem__(tuple(setitem_list),temp)
    else: 
        #Only write the variable if it is not empty:
        if not temp.mask.all():
            output.variables[var_name].__setitem__(tuple(setitem_list),temp)
    return output


def replicate_and_copy_variable(output,data,var_name,
                                datatype=None,fill_value=None,
                                add_dim=None,
                                chunksize=None,zlib=None,hdf5=None,check_empty=False):

    replicate_netcdf_var(output,data,var_name,
                        datatype=datatype,fill_value=fill_value,
                        add_dim=add_dim,
                        chunksize=chunksize,zlib=zlib)

    if len(data.variables[var_name].dimensions)==0:
        #scalar variable:
        output.variables[var_name][:]=data.variables[var_name][:]
        return output

    variable_size=min(data.variables[var_name].shape)
    storage_size=variable_size
    #Use the hdf5 library to find the real size of the stored array:
    if hdf5!=None:
        variable_size=hdf5[var_name].size
        storage_size=hdf5[var_name].id.get_storage_size()

    if variable_size>0 and storage_size>0:
        max_request=450.0 #maximum request in Mb
        #max_request=9000.0 #maximum request in Mb
        max_time_steps=max(
                        int(np.floor(max_request*1024*1024/(32*np.prod(data.variables[var_name].shape[1:])))),
                        1)

        num_time_chunk=int(np.ceil(data.variables[var_name].shape[0]/float(max_time_steps)))
        for time_chunk in range(num_time_chunk):
            time_slice=slice(time_chunk*max_time_steps,
                             min((time_chunk+1)*max_time_steps,data.variables[var_name].shape[0])
                             ,1)
            output=copy_data_time_slice(data,output,var_name,time_slice,check_empty)
    return output

def copy_data_time_slice(data,output,var_name,time_slice,check_empty):
    temp=data.variables[var_name][time_slice,...]
    #Assign only if not masked everywhere:
    if not 'mask' in dir(temp) or not check_empty:
        output.variables[var_name][time_slice,...]=temp
    else: 
        #Only write the variable if it is not empty:
        if not temp.mask.all():
            output.variables[var_name][time_slice,...]=temp
    return output

def replicate_group(output,data,group_name):
    output_grp=create_group(output,data,group_name)
    replicate_netcdf_file(output_grp,data.groups[group_name])
    return output_grp

def create_group(output,data,group_name):
    if not group_name in output.groups.keys():
        output_grp=output.createGroup(group_name)
    else:
        output_grp=output.groups[group_name]
    return output_grp
    
def replicate_netcdf_file_safe(data,output):
    return replicate_netcdf_file(output,data)

def replicate_netcdf_file(output,data):
    for att in data.ncattrs():
        att_val=data.getncattr(att)
        if 'encode' in dir(att_val):
            att_val=str(att_val.encode('ascii','replace'))
        if (not att in output.ncattrs() and
            att != 'cdb_query_temp'):
            try:
                setattr(output,att,att_val)
            except:
                output.setncattr(att,att_val)
    return output


def replicate_netcdf_var_dimensions(output,data,var,
                        datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None):
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys() and dims in data.dimensions.keys():
            if data.dimensions[dims].isunlimited():
                output.createDimension(dims,None)
            else:
                output.createDimension(dims,len(data.dimensions[dims]))
            if dims in data.variables.keys():
                output = replicate_netcdf_var(output,data,dims,zlib=True)
                output.variables[dims][:]=data.variables[dims][:]
                if ('bounds' in output.variables[dims].ncattrs() and
                    output.variables[dims].getncattr('bounds') in data.variables.keys()):
                    output=replicate_netcdf_var(output,data,output.variables[dims].getncattr('bounds'),zlib=True)
                    output.variables[output.variables[dims].getncattr('bounds')][:]=data.variables[output.variables[dims].getncattr('bounds')][:]
            else:
                #Create a dummy dimension variable:
                dim_var = output.createVariable(dims,np.float,(dims,),chunksizes=(1,))
                dim_var[:]=np.arange(len(data.dimensions[dims]))
    return output

def replicate_netcdf_other_var_safe(data,output,var,time_dim):
    #Replicates all variables except specified variable:
    variables_list=[ other_var for other_var in variables_list_with_time_dim(data,time_dim)
                                if other_var!=var]
    for other_var in variables_list:
        output=replicate_netcdf_var(output,data,other_var)
    return output

def replicate_netcdf_var_safe(data,output,var):
    return  replicate_netcdf_var(output,data,var,chunksize=-1,zlib=True)

def replicate_netcdf_var(output,data,var,
                        datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None):
    if not var in data.variables.keys():
        return output

    output=replicate_netcdf_var_dimensions(output,data,var)
    if var in output.variables.keys():
        #var is a dimension variable and does not need to be created:
        return output

    if datatype==None: datatype=data.variables[var].datatype
    if (isinstance(datatype,netCDF4.CompoundType) and
        not datatype.name in output.cmptypes.keys()):
        datatype=output.createCompoundType(datatype.dtype,datatype.name)

    kwargs=dict()
    if (fill_value==None and 
        '_FillValue' in dir(data.variables[var]) and 
        datatype==data.variables[var].datatype):
            kwargs['fill_value']=data.variables[var]._FillValue
    else:
        kwargs['fill_value']=fill_value

    if zlib==None:
        if data.variables[var].filters()==None:
            kwargs['zlib']=False
        else:
            for item in data.variables[var].filters():
                kwargs[item]=data.variables[var].filters()[item]
    else:
        kwargs['zlib']=zlib
    
    if var not in output.variables.keys():
        dimensions=data.variables[var].dimensions
        time_dim=find_time_dim(data)
        if add_dim:
            dimensions+=(add_dim,)
        if chunksize==-1:
            chunksizes=tuple([1 if dim==time_dim else data.variables[var].shape[dim_id] for dim_id,dim in enumerate(dimensions)])
        elif data.variables[var].chunking()=='contiguous':
            #chunksizes=tuple([1 if output.dimensions[dim].isunlimited() else 10 for dim in dimensions])
            #chunksizes=tuple([1 if dim==time_dim else chunksize for dim in dimensions])
            #chunksizes=tuple([1 if dim==time_dim else chunksize for dim_id,dim in enumerate(dimensions)])
            chunksizes=tuple([1 for dim_id,dim in enumerate(dimensions)])
        else:
            chunksizes=data.variables[var].chunking()
        kwargs['chunksizes']=chunksizes
        output.createVariable(var,datatype,dimensions,**kwargs)
    output = replicate_netcdf_var_att(output,data,var)
    return output

def replicate_netcdf_var_att(output,data,var):
    for att in data.variables[var].ncattrs():
        att_val=data.variables[var].getncattr(att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            if 'encode' in dir(att):
                att=att.encode('ascii','replace')
            try:
                setattr(output.variables[var],att,att_val)
            except:
                output.variables[var].setncattr(att,att_val)
    return output

def create_time_axis(output,data,time_axis):
    #output.createDimension(time_dim,len(time_axis))
    time_dim=find_time_dim(data)
    output.createDimension(time_dim,None)
    time = output.createVariable(time_dim,'d',(time_dim,),chunksizes=(1,))
    if data==None:
        time.calendar='standard'
        time.units='days since '+str(time_axis[0])
    else:
        time.calendar=netcdf_calendar(data)
        time_var=find_time_var(data)
        time.units=str(data.variables[time_var].units)
    time[:]=time_axis
    return

def create_time_axis_date(output,time_axis,units,calendar,time_dim='time'):
    #output.createDimension(time_dim,len(time_axis))
    output.createDimension(time_dim,None)
    time = output.createVariable(time_dim,'d',(time_dim,),chunksizes=(1,))
    time.calendar=calendar
    time.units=units
    time[:]=get_time_axis_relative(time_axis,time.units,time.calendar)
    #time[:]=netCDF4.date2num(time_axis,time.units,calendar=time.calendar)
    return

def netcdf_calendar(data):
    time_var=find_time_var(data)
    if 'calendar' in data.variables[time_var].ncattrs():
        calendar=data.variables[time_var].calendar
    else:
        calendar='standard'
    if 'encode' in dir(calendar):
        calendar=calendar.encode('ascii','replace')
    return calendar

def find_time_var(data):
    var_list=data.variables.keys()
    return find_time_name_from_list(var_list)

def find_time_dim(data):
    dim_list=data.dimensions.keys()
    return find_time_name_from_list(dim_list)

def find_time_name_from_list(list_of_names):
    try:
        return list_of_names[next(i for i,v in enumerate(list_of_names) if v.lower() == 'time')]
    except StopIteration:
        return None

def variables_list_with_time_dim(data,time_dim):
    return [ var for var in data.variables.keys() if time_dim in data.variables[var].dimensions]

def find_dimension_type(data):
    dimensions=data.dimensions
    time_dim=find_time_name_from_list(dimensions.keys())
    dimension_type=OrderedDict()
    for dim in dimensions.keys():
        if dim!=time_dim:
            dimension_type[dim]=len(dimensions[dim])
    return dimension_type

def netcdf_time_units(data):
    time_var=find_time_var(data)
    if 'units' in dir(data.variables[time_var]):
        units=data.variables[time_var].units
    else:
        units=None
    return units

def retrieve_dimension(data,dimension):
    attributes=dict()
    if dimension in data.variables.keys():
        #Retrieve attributes:
        for att in data.variables[dimension].ncattrs():
            attributes[att]=data.variables[dimension].getncattr(att)
        #If dimension is available, retrieve
        dimension_data = data.variables[dimension][:]
    else:
        #If dimension is not avaiable, create a simple indexing dimension
        dimension_data = np.arange(len(data.dimensions[dimension]))
    return dimension_data, attributes

def retrieve_dimension_list(data,var):
    return data.variables[var].dimensions

def retrieve_variables_safe(data,output,zlib=True):
    return retrieve_variables(output,data,zlib=zlib)

def retrieve_variables(output,data,zlib=True):
    for var_name in data.variables.keys():
        output=replicate_and_copy_variable(output,data,var_name,zlib=zlib,check_empty=False)
    return output

def retrieve_variables_no_time_safe(data,output,time_dim,zlib=True):
    return retrieve_variables_no_time(output,data,time_dim,zlib=zlib)

def retrieve_variables_no_time(output,data,time_dim,zlib=False):
    for var in data.variables.keys():
        if ( (not time_dim in data.variables[var].dimensions) and 
             (not var in output.variables.keys())):
            replicate_and_copy_variable(output,data,var)
    return output


def grab_indices(data,var,indices,unsort_indices):
    dimensions=retrieve_dimension_list(data,var)
    return retrieve_slice(data.variables[var],indices,unsort_indices,dimensions[0],dimensions[1:],0)

def create_date_axis_from_time_axis(time_axis,attributes_dict):
    units=attributes_dict['units']
    if 'calendar' in attributes_dict.keys(): 
        calendar=attributes_dict['calendar']
    else:
        calendar='standard'

    if units=='day as %Y%m%d.%f':
        date_axis=np.array(map(convert_to_date_absolute,native_time_axis))
    else:
        try:
            #Put cmip5_rewrite_time_axis here:
            date_axis=get_date_axis_relative(time_axis,units,calendar)
            #date_axis=netCDF4.num2date(time_axis,units=units,calendar=calendar)
        except TypeError:
            date_axis=np.array([]) 
    return date_axis


def retrieve_slice(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple(),num_trials=2):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        try:
            return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_tuple+(x,)),
                                                     indices[dim]),
                                  axis=dim_id),unsort_indices[dim],axis=dim_id)
        except RuntimeError:
            time.sleep(15)
            if num_trials>0:
                return retrieve_slice(variable,
                                        indices,
                                            unsort_indices,
                                            dim,dimensions,
                                            dim_id,
                                            getitem_tuple=getitem_tuple,
                                            num_trials=num_trials-1)
            else:
                raise 

def getitem_pedantic(shape,getitem_tuple):
    getitem_tuple_fixed=()
    for item_id, item in enumerate(getitem_tuple):
        indices_list=range(shape[item_id])[item]
        if indices_list[-1]+item.step>shape[item_id]:
            #Must fix the slice:
            #getitem_tuple_fixed+=(slice(item.start,shape[item_id],item.step),)
            getitem_tuple_fixed+=(indices_list,)
        else:
            getitem_tuple_fixed+=(item,)
    return getitem_tuple_fixed
        
def retrieve_slice_pedantic(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice_pedantic(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        shape=variable.shape
        return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_pedantic(variable.shape,getitem_tuple+(x,))),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)

