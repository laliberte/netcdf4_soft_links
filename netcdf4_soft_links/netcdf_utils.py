#External:
import numpy as np
import math
import time
import h5netcdf.legacyapi as netCDF4_h5
import netCDF4
import h5py
import datetime
import copy
import os
from collections import OrderedDict

#Internal:
from . import indices_utils

def check_if_opens(dataset,default=False):
    if default: return False
    return True

def get_year_axis(dataset,default=False):
    if default: return np.array([]),np.array([])

    dimensions_list = dataset.dimensions.keys()
    time_dim = find_time_dim(dataset)
    date_axis = get_date_axis(dataset, time_dim)
    year_axis = np.array([date.year for date in date_axis])
    month_axis = np.array([date.month for date in date_axis])
    return year_axis, month_axis

def get_date_axis(dataset,time_dim,default=False):
    if default: return np.array([])
    
    #Use np.asscalar(np.asarray(x)) to ensure that attributes are not arrays if lenght-1
    units = getncattr(dataset.variables[time_dim], 'units')
    if 'calendar' in dataset.variables[time_dim].ncattrs():
        calendar = getncattr(dataset.variables[time_dim], 'calendar')
    else:
        calendar = None
    return get_date_axis_from_units_and_calendar(dataset.variables[time_dim][:], units, calendar)

def get_date_axis_from_units_and_calendar(time_axis, units, calendar, default=False):
    if default: return np.array([])

    if units == 'day as %Y%m%d.%f':
        date_axis = get_date_axis_absolute(time_axis)
    else:
        date_axis = get_date_axis_relative(time_axis, units, calendar)
    return date_axis

def get_date_axis_relative(time_axis,units,calendar,default=False):
    if default: return np.array([])
    if calendar is not None:
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

def get_date_axis_absolute(time_axis,default=False):
    if default: return np.array([])
    return map(convert_to_date_absolute,time_axis)

def get_time(dataset,time_var='time',default=False):
    if default: return np.array([])
    time_dim = find_time_dim(dataset,time_var=time_var)
    time_axis, attributes = retrieve_dimension(dataset, time_dim)
    date_axis = create_date_axis_from_time_axis(time_axis, attributes)
    return date_axis

def get_time_axis_relative(date_axis,units,calendar,default=False):
    if default: return np.array([])
    if calendar != None:
        try:
            time_axis = netCDF4.date2num(date_axis,units=units,calendar=calendar)
        except ValueError:
            if (
                (units == 'days since 0-01-01 00:00:00' and
                calendar == '365_day') or
                (units == 'days since 0-1-1 00:00:00' and
                calendar == '365_day') 
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

def replicate_full_netcdf_recursive(dataset,output,transform=(lambda x,y,z:y),slices=dict(),
                                                check_empty=False,default=False):
    if default: return output

    for var_name in dataset.variables.keys():
        replicate_and_copy_variable(dataset,output,var_name,transform=transform,slices=slices,check_empty=check_empty)
    if len(dataset.groups.keys())>0:
        for group in dataset.groups.keys():
            output_grp = replicate_group(dataset,output,group)
            replicate_full_netcdf_recursive(dataset.groups[group],output_grp,transform=transform,slices=slices,check_empty=check_empty)
    return output

def dimension_compatibility(dataset,output,dim,default=False):
    if default: return False

    if (dim in output.dimensions.keys()
        and _dim_len(output,dim)!=_dim_len(dataset,dim)):
        #Dimensions mismatch, return without writing anything
        return False
    elif ( (dim in dataset.variables.keys() and
          dim in output.variables.keys()) and
          ( len(output.variables[dim]) != len(dataset.variables[dim]) or 
           (dataset.variables[dim][:] != dataset.variables[dim][:]).any())):
        #Dimensions variables mismatch, return without writing anything
        return False
    else:
        return True

def check_dimensions_compatibility(dataset,output,var_name,exclude_unlimited=False,default=False):
    if default: return False
    for dim in dataset.variables[var_name].dimensions:
        #The dimensions might be in the parent group:
        if not dim in dataset.dimensions.keys():
            dataset_parent = dataset.parent
        elif not dim in dataset.variables.keys():
            #Important check for h5netcdf
            dataset_parent = dataset.parent
        else:
            dataset_parent = dataset

        if not dim in output.dimensions.keys():
            output_parent = output.parent
        else:
            output_parent = output

        if not _isunlimited(dataset_parent,dim) or not exclude_unlimited:
            if not dimension_compatibility(dataset_parent,output_parent,dim):
                return False
    return True

def _isunlimited(dataset,dim):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
        isinstance(dataset, netCDF4_h5.Group)):
        var_list_with_dim=[var for var in dataset.variables.keys() 
                                if dim in dataset.variables[var].dimensions]
        if len(var_list_with_dim)==0:
            return False

        if np.all([dataset._h5group[var].maxshape[
                                    list(dataset.variables[var].dimensions).index(dim)]==None
                                for var in var_list_with_dim]):
            #If the maxshape of dimension for all variables with dimenion is None, it is unlimited!
            return True
        else:
            return False
    else:
        return dataset.dimensions[dim].isunlimited()

def _dim_len(dataset,dim):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
        isinstance(dataset, netCDF4_h5.Group)):
        return dataset.dimensions[dim]
    else:
        return len(dataset.dimensions[dim])

def _datatype(dataset,var):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
        isinstance(dataset, netCDF4_h5.Group)):
        dtype = dataset.variables[var].dtype
        if dtype == 'object':
            return str
        else:
            return dtype
    else:
        return dataset.variables[var].datatype

def append_record(dataset,output,default=False):
    record_dimensions = dict()
    if default: return record_dimensions
    for dim in dataset.dimensions.keys():
        if (     dim in dataset.variables.keys()
             and dim in output.dimensions.keys()
             and dim in output.variables.keys()
             and _isunlimited(dataset,dim)):
             append_slice = slice(_dim_len(output,dim),_dim_len(output,dim)+
                                                          _dim_len(dataset,dim),1)
             ensure_compatible_time_units(output,dataset,dim)
             temp = dataset.variables[dim][:]
             output.variables[dim][append_slice] = temp
             record_dimensions[dim] = {'append_slice':append_slice}
    return record_dimensions

def ensure_compatible_time_units(dataset,output,dim,default=False):
    if default: return None
    #Use np.asscalar(np.asarray()) for backward and forward compatibility:
    for time_desc in ['units','calendar']:
        if ( (     time_desc in output.variables[dim].ncattrs()
               and time_desc in dataset.variables[dim].ncattrs()) and
              ( getncattr(output.variables[dim], time_desc) != 
                getncattr(dataset.variables[dim], time_desc)
              )):
            raise 'time units and calendar must be the same when appending soft links'
    return 


def append_and_copy_variable(dataset,output,var_name,record_dimensions,
                             datatype=None,fill_value=None,add_dim=None,
                             chunksize=None,zlib=False,check_empty=False,default=False):
    if default: return output

    if len(set(record_dimensions.keys()).intersection(dataset.variables[var_name].dimensions))==0:
        #Variable does not contain a record dimension, return
        return output
   
    variable_size = min(dataset.variables[var_name].shape)
    storage_size = variable_size
    if '_h5ds' in dir(dataset):
        #Use the hdf5 library to find the real size of the stored array:
        variable_size = dataset.variables[var_name]._h5ds.size
        storage_size = dataset.variables[var_name]._h5ds.id.get_storage_size()

    if variable_size > 0 and storage_size > 0:
        max_request = 450.0 #maximum request in Mb
        max_first_dim_steps = max(
                        int(np.floor(max_request*1024*1024/(32*np.prod(dataset.variables[var_name].shape[1:])))),
                        1)

        num_first_dim_chunk = int(np.ceil(dataset.variables[var_name].shape[0]/float(max_first_dim_steps)))
        for first_dim_chunk in range(num_first_dim_chunk):
            first_dim_slice = slice(first_dim_chunk*max_first_dim_steps,
                             min((first_dim_chunk+1)*max_first_dim_steps,dataset.variables[var_name].shape[0])
                             ,1)
            output = append_dataset_first_dim_slice(dataset,output,var_name,first_dim_slice,record_dimensions,check_empty)
    return output

def append_dataset_first_dim_slice(dataset,output,var_name,first_dim_slice,record_dimensions,check_empty):
    #Create a setitem tuple
    setitem_list = [ slice(0,_dim_len(dataset,dim),1) if not dim in record_dimensions.keys()
                                                               else record_dimensions[dim]['append_slice']
                                                              for dim in dataset.variables[var_name].dimensions]
    #Pick a first_dim_slice along the first dimension:
    setitem_list[0] = indices_utils.slice_a_slice(setitem_list[0], first_dim_slice)
    temp = dataset.variables[var_name][first_dim_slice, ...]
    #Assign only if not masked everywhere:
    if ( not 'mask' in dir(temp) or 
         not check_empty ):
        output.variables[var_name].__setitem__(tuple(setitem_list), temp)
    else: 
        #Only write the variable if it is not empty:
        if not temp.mask.all():
            output.variables[var_name].__setitem__(tuple(setitem_list), temp)
    return output

def replicate_and_copy_variable(dataset,output,var_name,
                                datatype=None,fill_value=None,
                                add_dim=None,
                                chunksize=None,zlib=False,
                                transform=(lambda x,y,z:y),
                                slices=dict(),
                                check_empty=False,default=False):

    if default: return output

    if not isinstance(slices,dict):
        #assume it is a function that takes the dataset as input and outputs
        #a slicing dict
        comp_slices = slices(dataset)
    else:
        comp_slices = slices

    replicate_netcdf_var(dataset, output, var_name,
                        datatype=datatype, fill_value=fill_value,
                        add_dim=add_dim,
                        slices=comp_slices,
                        chunksize=chunksize, zlib=zlib)

    #Apply a transformation if dimensions are in slices:
    if set(comp_slices.keys()).issubset(dataset.variables[var_name].dimensions):
        transform(dataset, output, comp_slices)

    if len(dataset.variables[var_name].dimensions) == 0:
        #scalar variable:
        value = dataset.variables[var_name][...]
        if not np.ma.is_masked(value):
            #if not masked, assign. Otherwise, do nothing
            output.variables[var_name][...] = value
        return output

    variable_size = min(dataset.variables[var_name].shape)
    storage_size = variable_size
    if '_h5ds' in dir(dataset):
        #Use the hdf5 library to find the real size of the stored array:
        variable_size = dataset.variables[var_name]._h5ds.size
        storage_size = dataset.variables[var_name]._h5ds.id.get_storage_size()

    if variable_size > 0 and storage_size > 0:
        max_request = 450.0 #maximum request in Mb
        #max_request=9000.0 #maximum request in Mb

        #Create the output variable shape, allowing slices:
        var_shape = tuple([ dataset.variables[var_name].shape[dim_id] if not dim in comp_slices.keys()
                            else len(np.arange(dataset.variables[var_name].shape[dim_id])[comp_slices[dim]])
                            for dim_id,dim in enumerate(dataset.variables[var_name].dimensions) ])
        max_first_dim_steps = max(
                        int(np.floor(max_request*1024*1024/(32*np.prod(var_shape[1:])))),
                        1)

        num_first_dim_chunk = int(np.ceil(var_shape[0]/float(max_first_dim_steps)))

        for first_dim_chunk in range(num_first_dim_chunk):
            first_dim_slice = slice(first_dim_chunk*max_first_dim_steps,
                                    min((first_dim_chunk+1)*max_first_dim_steps,var_shape[0])
                                    ,1)
            output = copy_dataset_first_dim_slice(dataset, output, var_name, first_dim_slice,
                                                  check_empty,slices=comp_slices)
    return output

def copy_dataset_first_dim_slice(dataset, output, var_name, first_dim_slice, check_empty, slices=dict()):
    combined_slices = slices.copy()
    first_dim = dataset.variables[var_name].dimensions[0]
    if first_dim in combined_slices:
        combined_slices[first_dim] = indices_utils.slice_a_slice(combined_slices[first_dim], first_dim_slice)
    else:
        combined_slices[first_dim] = first_dim_slice
                
    getitem_tuple = tuple([ combined_slices[var_dim] if var_dim in combined_slices.keys()
                            else slice(None,None,1) for var_dim in
                            dataset.variables[var_name].dimensions ])

    temp = dataset.variables[var_name][getitem_tuple]
    #Assign only if not masked everywhere:
    if ( not 'mask' in dir(temp) or
         not check_empty ):
        output.variables[var_name][first_dim_slice, ...] = temp
    else: 
        #Only write the variable if it is not empty:
        if not temp.mask.all():
            output.variables[var_name][first_dim_slice, ...] = temp
    return output

def replicate_group(dataset, output, group_name, default=False):
    if default: return output
    output_grp = create_group(dataset, output, group_name)
    replicate_netcdf_file(dataset.groups[group_name], output_grp)
    return output_grp

def create_group(dataset, output, group_name, default=False):
    if default: return output
    if not group_name in output.groups.keys():
        output_grp = output.createGroup(group_name)
    else:
        output_grp = output.groups[group_name]
    return output_grp
    
def replicate_netcdf_file(dataset, output, default=False):
    if default: return output

    for att in dataset.ncattrs():
        #Use np.asscalar(np.asarray()) for backward and forward compatibility:
        att_val = _toscalar(getncattr(dataset, att))
        
        #This fix is for compatitbility with h5netcdf:
        if ( 'dtype' in dir(att_val) and
            att_val.dtype == np.dtype('O')):
            if len(att_val) == 1:
                att_val = att_val[0]
            else:
                att_val = np.asarray(att_val, dtype='str')

        if 'encode' in dir(att_val):
            att_val = str(att_val.encode('ascii', 'replace'))
            
        if (not att in output.ncattrs() and
            att != 'cdb_query_temp'):
            setncattr(output, att, att_val)
    return output

def setncattr(output, att, att_val):
    if isinstance(att_val, str):
        try:
            output.setncattr_string(att, att_val)
        except AttributeError as e:
            output.setncattr(att, att_val)
    else:
        try:
            setattr(output,att,att_val)
        except Exception:
            output.setncattr(att,att_val)
    return

def getncattr(dataset, att):
    return np.asscalar(np.asarray(dataset.getncattr(att)))

def _is_dimension_present(dataset,dim):
    if dim in dataset.dimensions:
        return True
    elif dataset.parent is not None:
        return _is_dimension_present(dataset.parent, dim)
    else:
        return False

def replicate_netcdf_var_dimensions(dataset, output, var,
                                    slices=dict(),
                                    datatype=None,
                                    fill_value=None,
                                    add_dim=None,
                                    chunksize=None,zlib=False,default=False):
    if default: return output
    for dims in dataset.variables[var].dimensions:
        if ( not _is_dimension_present(output, dims) and 
             _is_dimension_present(dataset, dims) ):
            if _isunlimited(dataset, dims):
                output.createDimension(dims, None)
            elif dims in slices.keys():
                output.createDimension(dims, len(np.arange(_dim_len(dataset, dims))[slices[dims]]))
            else:
                output.createDimension(dims, _dim_len(dataset, dims))
            if dims in dataset.variables:
                replicate_netcdf_var(dataset, output, dims, zlib=True, slices=slices)
                if dims in slices.keys():
                    output.variables[dims][:] = dataset.variables[dims][slices[dims]]
                else:
                    output.variables[dims][:] = dataset.variables[dims][:]
                if ('bounds' in output.variables[dims].ncattrs() and
                    getncattr(output.variables[dims], 'bounds')
                    in dataset.variables.keys()):
                    var_bounds = getncattr(output.variables[dims], 'bounds')
                    if not var_bounds in output.variables.keys():
                        output=replicate_netcdf_var(dataset, output, var_bounds,
                                                    zlib=True, slices=slices)
                        if dims in slices.keys():
                            getitem_tuple = tuple([ slices[var_bounds_dim] if var_bounds_dim in slices.keys()
                                                  else slice(None,None,1) for var_bounds_dim in
                                                  dataset.variables[var_bounds].dimensions ])
                            output.variables[var_bounds][:] = dataset.variables[var_bounds][getitem_tuple]
                        else:
                            output.variables[var_bounds][:] = dataset.variables[var_bounds][:]
            else:
                #Create a dummy dimension variable:
                dim_var = output.createVariable(dims,np.float,(dims,),chunksizes=(1,))
                if dims in slices.keys():
                    dim_var[:]=np.arange(_dim_len(dataset,dims))[slices[dims]]
                else:
                    dim_var[:]=np.arange(_dim_len(dataset,dims))
    return output

def replicate_netcdf_other_var(dataset,output,var,time_dim,default=False):
    if default: return output
    #Replicates all variables except specified variable:
    variables_list=[ other_var for other_var in variables_list_with_time_dim(dataset,time_dim)
                                if other_var!=var]
    for other_var in variables_list:
        output=replicate_netcdf_var(dataset,output,other_var)
    return output

def replicate_netcdf_var(dataset,output,var,
                        slices=dict(),
                        datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=False,default=False):
    if default: 
        #Create empty variable:
        output.createVariable(var,'d',())
        return output

    if not var in dataset.variables.keys():
        return output

    output=replicate_netcdf_var_dimensions(dataset,output,var,slices=slices)
    if var in output.variables.keys():
        #var is a dimension variable and does not need to be created:
        return output

    if datatype==None: datatype=_datatype(dataset,var)
    if (isinstance(datatype,netCDF4.CompoundType) and
        not datatype.name in output.cmptypes.keys()):
        datatype=output.createCompoundType(datatype.dtype,datatype.name)

    #Weird fix for strings:
    #if 'str' in dir(datatype) and 'S1' in datatype.str:
    #    datatype='S1'

    kwargs=dict()
    if (fill_value==None and 
        '_FillValue' in dataset.variables[var].ncattrs() and 
        datatype==_datatype(dataset,var)):
            kwargs['fill_value'] = getncattr(dataset.variables[var], '_FillValue')
    else:
        kwargs['fill_value'] = fill_value

    if not zlib:
        if dataset.variables[var].filters()==None:
            kwargs['zlib']=False
        else:
            for item in dataset.variables[var].filters():
                kwargs[item]=dataset.variables[var].filters()[item]
    else:
        kwargs['zlib']=zlib
    
    if var not in output.variables.keys():
        dimensions=dataset.variables[var].dimensions
        time_dim=find_time_dim(dataset)
        if add_dim:
            dimensions+=(add_dim,)
        var_shape=tuple([dataset.variables[var].shape[dim_id] if not dim in slices.keys()
                                               else len(np.arange(dataset.variables[var].shape[dim_id])[slices[dim]])
                                               for dim_id,dim in enumerate(dimensions)])
        if chunksize==-1:
            chunksizes=tuple([1 if dim==time_dim else var_shape[dim_id] for dim_id,dim in enumerate(dimensions)])
        elif dataset.variables[var].chunking()=='contiguous':
            if kwargs['zlib']:
                chunksizes=tuple([1 if dim==time_dim else var_shape[dim_id] for dim_id,dim in enumerate(dimensions)])
            else:
                chunksizes=tuple([1 for dim_id,dim in enumerate(dimensions)])
        else:
            if len(set(dimensions).intersection(slices.keys()))>0:
                if kwargs['zlib']:
                    chunksizes=tuple([1 if dim==time_dim else var_shape[dim_id] for dim_id,dim in enumerate(dimensions)])
                else:
                    chunksizes=tuple([1 for dim_id,dim in enumerate(dimensions)])
            else:
                chunksizes=dataset.variables[var].chunking()
        kwargs['chunksizes']=chunksizes
        out_var=output.createVariable(var,datatype,dimensions,**kwargs)
    output = replicate_netcdf_var_att(dataset,output,var)
    return output
    #return out_var

def _toscalar(x):
    try:
        return np.asscalar(x)
    except (AttributeError, ValueError) as e:
        return x

def replicate_netcdf_var_att(dataset,output,var,default=False):
    if default: return output
    for att in dataset.variables[var].ncattrs():
        #Use np.asscalar(np.asarray()) for backward and forward compatibility:
        att_val = _toscalar(getncattr(dataset.variables[var], att))
        if isinstance(att_val,dict):
            atts_pairs=[(att+'.'+key,att_val[key]) for key in att_val.keys()]
        else:
            atts_pairs=[(att,att_val)]
        for att_pair in atts_pairs:
            if att_pair[0][0]!='_':
                if 'encode' in dir(att_pair[1]):
                    att_val=att_pair[1].encode('ascii','replace')
                else:
                    att_val=att_pair[1]
                if 'encode' in dir(att_pair[0]):
                    att=att_pair[0].encode('ascii','replace')
                else:
                    att=att_pair[0]
                setncattr(output.variables[var],att,att_val)
    return output

def create_time_axis(dataset,output,time_axis,time_var='time',default=False):
    if default: return output
    #output.createDimension(time_dim,len(time_axis))
    time_dim=find_time_dim(dataset,time_var=time_var)
    output.createDimension(time_dim,None)
    time = output.createVariable(time_dim,'d',(time_dim,),chunksizes=(1,))
    if dataset == None:
        setncattr(time, 'calendar', 'standard')
        setncattr(time, 'units', 'days since '+str(time_axis[0]))
    else:
        setncattr(time, 'calendar', netcdf_calendar(dataset, time_var=time_var))
        time_var = find_time_var(dataset, time_var=time_var)
        #Use np.asscalar(np.asarray()) for backward and forward compatibility:
        setncattr(time, 'units', str(getncattr(dataset.variables[time_var], 'units')))
    time[:] = time_axis
    return output

def create_time_axis_date(output, time_axis, units, calendar, time_dim='time'):
    output.createDimension(time_dim, None)
    time = output.createVariable(time_dim, 'd', (time_dim,), chunksizes=(1,))
    setncattr(time, 'calendar', calendar)
    setncattr(time, 'units', units)
    time[:] = get_time_axis_relative(time_axis, getncattr(time, 'units'),
                                                getncattr(time, 'calendar') )
    return

def netcdf_calendar(dataset, time_var='time', default=False):
    calendar = 'standard'
    if default: return calendar

    time_var = find_time_var(dataset, time_var=time_var)
    if time_var is not None:
        if 'calendar' in dataset.variables[time_var].ncattrs():
            #Use np.asscalar(np.asarray()) for backward and forward compatibility:
            calendar = getncattr(dataset.variables[time_var], 'calendar')
        if 'encode' in dir(calendar):
            calendar = calendar.encode('ascii', 'replace')
    return calendar
    
def find_time_var(dataset, time_var='time', default=False):
    if default: return time_var
    var_list = dataset.variables.keys()
    return find_time_name_from_list(var_list, time_var)

def find_time_dim(dataset, time_var='time', default=False):
    if default: return time_var
    dim_list = dataset.dimensions.keys()
    return find_time_name_from_list(dim_list, time_var)

def find_time_name_from_list(list_of_names, time_var):
    try:
        return list_of_names[ next(i for i,v in enumerate(list_of_names) 
                              if v.lower() == time_var) ]
    except StopIteration:
        return None

def variables_list_with_time_dim(dataset, time_dim, default=False):
    if default: return []
    return [ var for var in dataset.variables.keys()
             if time_dim in dataset.variables[var].dimensions ]

def find_dimension_type(dataset, time_var='time', default=False):
    dimension_type = OrderedDict()
    if default: return dimension_type

    time_dim = find_time_name_from_list(dataset.dimensions.keys(), time_var)
    for dim in dataset.dimensions.keys():
        if dim != time_dim:
            dimension_type[dim] = _dim_len(dataset,dim)
    return dimension_type

def netcdf_time_units(dataset, time_var='time', default=False):
    units = None
    if default: return units
    time_var = find_time_var(dataset, time_var=time_var)
    if 'units' in dataset.variables[time_var].ncattrs():
        #Use np.asscalar(np.asarray()) for backward and forward compatibility:
        units = getncattr(dataset.variables[time_var], 'units')
    return units

def retrieve_dimension(dataset, dimension, default=False):
    attributes = dict()
    dimension_dataset = np.array([])
    if default: return dimension_dataset, attributes

    if dimension in dataset.variables.keys():
        #Retrieve attributes:
        for att in dataset.variables[dimension].ncattrs():
            #Use np.asscalar(np.asarray()) for backward and forward compatibility:
            attributes[att] = getncattr(dataset.variables[dimension], att)
        #If dimension is available, retrieve
        dimension_dataset = dataset.variables[dimension][...]
    else:
        #If dimension is not avaiable, create a simple indexing dimension
        dimension_dataset = np.arange(_dim_len(dataset, dimension))
    return dimension_dataset, attributes

def retrieve_dimension_list(dataset, var, default=False):
    dimensions = tuple()
    if default: return dimensions
    return dataset.variables[var].dimensions

def retrieve_dimensions_no_time(dataset, var, time_var='time', default=False):
    dimensions_data = dict()
    attributes = dict()
    if default: return dimensions_data, attributes
    dimensions = retrieve_dimension_list(dataset, var)
    time_dim = find_time_name_from_list(dimensions, time_var)
    for dim in dimensions:
        if dim != time_dim:
            dimensions_data[dim], attributes[dim] = retrieve_dimension(dataset, dim)
    return dimensions_data, attributes

def retrieve_variables(dataset, output, zlib=True, default=False):
    if default: return output
    for var_name in dataset.variables.keys():
        output = replicate_and_copy_variable(dataset, output, var_name,
                                             zlib=zlib, check_empty=False)
    return output

def retrieve_variables_no_time(dataset, output, time_dim, zlib=False, default=False):
    if default: return output
    for var in dataset.variables.keys():
        if ( (not time_dim in dataset.variables[var].dimensions) and 
             (not var in output.variables.keys())):
            replicate_and_copy_variable(dataset, output, var, zlib=zlib)
    return output

def find_time_dim_and_replicate_netcdf_file(dataset, output, time_var='time', default=False):
    if default: return ( find_time_dim(dataset, time_var=time_var, default=True),
                         replicate_netcdf_file(dataset, output, default=True) )
    return ( find_time_dim(dataset, time_var=time_var),
             replicate_netcdf_file(dataset, output) )

def create_date_axis_from_time_axis(time_axis, attributes_dict, default=False):
    if default: return np.array([])

    calendar='standard'
    units = attributes_dict['units']
    if 'calendar' in attributes_dict.keys(): 
        calendar = attributes_dict['calendar']

    if units == 'day as %Y%m%d.%f':
        date_axis = np.array(map(convert_to_date_absolute,
                                 native_time_axis))
    else:
        try:
            #Put cmip5_rewrite_time_axis here:
            date_axis = get_date_axis_relative(time_axis, units, calendar)
        except TypeError:
            date_axis = np.array([]) 
    return date_axis

def retrieve_container(dataset, var, dimensions, unsort_dimensions,
                       sort_table, max_request, time_var='time',
                       file_name='', default=False):
    if default: return np.array([])
    remote_dimensions, attributes = retrieve_dimensions_no_time(dataset, var, time_var=time_var)

    indices = copy.copy(dimensions)
    unsort_indices = copy.copy(unsort_dimensions)
    for dim in remote_dimensions.keys():
        indices[dim], unsort_indices[dim] = indices_utils.prepare_indices(
                                            indices_utils.get_indices_from_dim(remote_dimensions[dim],
                                                                               indices[dim]))
    return grab_indices(dataset, var, indices, unsort_indices, max_request, file_name=file_name)

def grab_indices(dataset, var, indices, unsort_indices, max_request,
                 file_name='', default=False):
    if default: return np.array([])
    dimensions = retrieve_dimension_list(dataset,var)
    return indices_utils.retrieve_slice(dataset.variables[var], indices, unsort_indices,
                                        dimensions[0], dimensions[1:], 0, max_request)
