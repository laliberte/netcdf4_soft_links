# External:
import numpy as np
import math
import h5netcdf.legacyapi as netCDF4_h5
import netCDF4
import datetime
import copy
from collections import OrderedDict

# Internal:
from . import indices_utils
from . import netcdf_utils_defaults


def default(f):
    def func_wrapper(*args, **kwargs):
        new_kwargs = {key: kwargs[key] for key in kwargs
                      if key != 'default'}
        if ('default' in kwargs and
           kwargs['default']):
            # Get default:
            return getattr(netcdf_utils_defaults, f.__name__)(*args,
                                                              **new_kwargs)
        else:
            return f(*args, **new_kwargs)
    return func_wrapper


@default
def check_if_opens(dataset):
    return True


@default
def get_year_axis(dataset):
    time_dim = find_time_dim(dataset)
    date_axis = get_date_axis(dataset, time_dim)
    year_axis = np.array([date.year for date in date_axis])
    month_axis = np.array([date.month for date in date_axis])
    return year_axis, month_axis


@default
def get_date_axis(dataset, time_dim):
    # Use np.asscalar(np.asarray(x)) to ensure that attributes
    # are not arrays if lenght-1
    units = getncattr(dataset.variables[time_dim], 'units')
    if 'calendar' in dataset.variables[time_dim].ncattrs():
        calendar = getncattr(dataset.variables[time_dim], 'calendar')
    else:
        calendar = None
    return get_date_axis_from_units_and_calendar(dataset
                                                 .variables[time_dim][:],
                                                 units, calendar)


@default
def get_date_axis_from_units_and_calendar(time_axis, units, calendar):
    if units == 'day as %Y%m%d.%f':
        date_axis = get_date_axis_absolute(time_axis)
    else:
        date_axis = get_date_axis_relative(time_axis, units, calendar)
    return date_axis


@default
def get_date_axis_relative(time_axis, units, calendar):
    if calendar is not None:
        try:
            date_axis = netCDF4.num2date(time_axis, units=units,
                                         calendar=calendar)
        except ValueError:
            if ((units == 'days since 0-01-01 00:00:00' and
                 calendar == '365_day') or
                (units == 'days since 0-1-1 00:00:00' and
                 calendar == '365_day')):
                date_axis = (netCDF4
                             .num2date(time_axis-365.0,
                                       units='days since 1-01-01 00:00:00',
                                       calendar=calendar))
            else:  # pragme: no cover
                raise
    else:
        date_axis = netCDF4.num2date(time_axis, units=units)
    return date_axis


@default
def get_date_axis_absolute(time_axis):
    return map(convert_to_date_absolute, time_axis)


@default
def get_time(dataset, time_var='time'):
    time_dim = find_time_dim(dataset, time_var=time_var)
    time_axis, attributes = retrieve_dimension(dataset, time_dim)
    date_axis = create_date_axis_from_time_axis(time_axis, attributes)
    return date_axis


@default
def get_time_axis_relative(date_axis, units, calendar):
    if calendar is not None:
        try:
            time_axis = netCDF4.date2num(date_axis, units=units,
                                         calendar=calendar)
        except ValueError:
            if ((units == 'days since 0-01-01 00:00:00' and
                 calendar == '365_day') or
                (units == 'days since 0-1-1 00:00:00' and
                 calendar == '365_day')):
                time_axis = (netCDF4
                             .date2num(date_axis,
                                       units='days since 1-01-01 00:00:00',
                                       calendar=calendar) + 365.0)
            else:
                raise
    else:
        time_axis = netCDF4.date2num(date_axis, units=units)
    return time_axis


def convert_to_date_absolute(absolute_time):
    year = int(math.floor(absolute_time/1e4))
    remainder = absolute_time-year*1e4
    month = int(math.floor(remainder/1e2))
    remainder -= month*1e2
    day = int(math.floor(remainder))
    remainder -= day
    remainder *= 24.0
    hour = int(math.floor(remainder))
    remainder -= hour
    remainder *= 60.0
    minute = int(math.floor(remainder))
    remainder -= minute
    remainder *= 60.0
    seconds = int(math.floor(remainder))
    return datetime.datetime(year, month, day,
                             hour, minute, seconds)


@default
def replicate_full_netcdf_recursive(dataset, output,
                                    transform=(lambda x, y, z: y),
                                    slices=dict(),
                                    check_empty=False):
    for var_name in dataset.variables:
        replicate_and_copy_variable(dataset, output, var_name,
                                    transform=transform,
                                    slices=slices,
                                    check_empty=check_empty)
    if len(dataset.groups.keys()) > 0:
        for group in dataset.groups:
            output_grp = replicate_group(dataset, output, group)
            replicate_full_netcdf_recursive(dataset.groups[group],
                                            output_grp,
                                            transform=transform,
                                            slices=slices,
                                            check_empty=check_empty)
    return output


@default
def dimension_compatibility(dataset, output, dim):

    if (dim in output.dimensions and
       _dim_len(output, dim) != _dim_len(dataset, dim)):
        # Dimensions mismatch, return without writing anything
        return False
    elif ((dim in dataset.variables and
           dim in output.variables) and
          (len(output.variables[dim]) != len(dataset.variables[dim]) or
           (dataset.variables[dim][:] != dataset.variables[dim][:]).any())):
        # Dimensions variables mismatch, return without writing anything
        return False
    else:
        return True


@default
def check_dimensions_compatibility(dataset, output, var_name,
                                   exclude_unlimited=False):
    for dim in dataset.variables[var_name].dimensions:
        # The dimensions might be in the parent group:
        if dim not in dataset.dimensions:
            dataset_parent = dataset.parent
        elif dim not in dataset.variables:
            # Important check for h5netcdf
            dataset_parent = dataset.parent
        else:
            dataset_parent = dataset

        if dim not in output.dimensions:
            output_parent = output.parent
        else:
            output_parent = output

        if (not _isunlimited(dataset_parent, dim) or
           not exclude_unlimited):
            if not dimension_compatibility(dataset_parent,
                                           output_parent,
                                           dim):
                return False
    return True


def _isunlimited(dataset, dim):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
       isinstance(dataset, netCDF4_h5.Group)):
        var_list_with_dim = [var for var in dataset.variables
                             if dim in dataset.variables[var].dimensions]
        if len(var_list_with_dim) == 0:
            return False

        if np.all([dataset
                   ._h5group[var]
                   .maxshape[list(dataset
                                  .variables[var]
                                  .dimensions).index(dim)] is None
                   for var in var_list_with_dim]):
            # If the maxshape of dimension for all variables with
            # dimension is None, it is unlimited!
            return True
        else:
            return False
    else:
        return dataset.dimensions[dim].isunlimited()


def _dim_len(dataset, dim):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
       isinstance(dataset, netCDF4_h5.Group)):
        return dataset.dimensions[dim]
    else:
        return len(dataset.dimensions[dim])


def _sanitized_datatype(dataset, var):
    try:
        datatype = dataset.variables[var].datatype
    except KeyError:
        datatype = dataset.variables[var].dtype
    if isinstance(datatype, np.dtype):
        try:
            return np.dtype(datatype.name)
        except TypeError:
            if 'S' in datatype.str:
                return np.dtype(str)
            else:
                return datatype
    else:
        return np.dtype(datatype)


@default
def append_record(dataset, output):
    record_dimensions = dict()
    for dim in dataset.dimensions:
        if (dim in dataset.variables and
            dim in output.dimensions and
            dim in output.variables and
           _isunlimited(dataset, dim)):
            append_slice = ensure_compatible_time_units(dataset, output, dim)
            record_dimensions[dim] = {'append_slice': append_slice}
    return record_dimensions


@default
def ensure_compatible_time_units(dataset, output, dim):
    try:
        units = dict()
        calendar = dict()
        for desc, data in [('source', dataset), ('dest', output)]:
            units[desc] = getncattr(data.variables[dim], 'units')
            if 'calendar' in data.variables[dim].ncattrs():
                calendar[desc] = getncattr(data.variables[dim], 'calendar')
            else:
                calendar[desc] = 'standard'

        converted_dim = (netCDF4
                         .date2num(netCDF4
                                   .num2date(dataset
                                             .variables[dim][:],
                                             units['source'],
                                             calendar=calendar['source']),
                                   units['dest'],
                                   calendar=calendar['dest']))

        dest_dim = output.variables[dim][:]
    except (KeyError, AttributeError):
        # 'calendar' or 'units' are not attributes
        converted_dim = dataset.variables[dim][:]
        dest_dim = output.variables[dim][:]

    overlapping_source_mask = np.in1d(converted_dim, dest_dim)
    if np.any(overlapping_source_mask):
        non_ovrlp_src_msk = np.invert(overlapping_source_mask)
        if sum(non_ovrlp_src_msk) > 0:
            appd_slc = slice(len(dest_dim),
                             len(dest_dim) +
                             sum(non_ovrlp_src_msk), 1)
            output.variables[dim][appd_slc] = converted_dim[non_ovrlp_src_msk]

            dest_dim = output.variables[dim][:]

        sort_dst_dim = np.argsort(dest_dim)
        appd_idx_or_slc = sort_dst_dim[np.searchsorted(dest_dim,
                                                       converted_dim,
                                                       sorter=sort_dst_dim)]
    else:
        appd_idx_or_slc = slice(len(dest_dim),
                                len(dest_dim) +
                                len(converted_dim), 1)
        output.variables[dim][appd_idx_or_slc] = converted_dim
    return appd_idx_or_slc


@default
def append_and_copy_variable(dataset, output, var_name, record_dimensions,
                             datatype=None, fill_value=None, add_dim=None,
                             chunksize=None, zlib=False, check_empty=False):

    if len(set(record_dimensions.keys())
           .intersection(dataset.variables[var_name].dimensions)) == 0:

        # Variable does not contain a record dimension, return
        return output

    if hasattr(dataset, '_h5ds'):
        # Use the hdf5 library to find the real size of the stored array:
        variable_size = dataset.variables[var_name]._h5ds.size
        storage_size = dataset.variables[var_name]._h5ds.id.get_storage_size()
    else:
        variable_size = min(dataset.variables[var_name].shape)
        storage_size = variable_size

    if variable_size > 0 and storage_size > 0:
        max_request = 450.0  # maximum request in Mb
        max_first_dim_steps = max(int(np.floor(max_request*1024*1024 /
                                               (32*np.prod(dataset
                                                           .variables[var_name]
                                                           .shape[1:])))), 1)

    # Using dask. Not working yet:
    #    source = da.from_array(dataset.variables[var_name],
    #                           chunks=(max_first_dim_steps,)+dataset.variables[var_name].shape[1:])
    #
    #    setitem_tuple = tuple([slice(0,_dim_len(dataset,dim),1)
    #                           if not dim in record_dimensions.keys()
    #                           else record_dimensions[dim]['append_slice']
    #                           for dim in dataset
    #                                      .variables[var_name]
    #                                      .dimensions ])
    #    dest = (da.from_array(output.variables[var_name],
    #                          chunks=(1,) + (output
    #                                         .variables[var_name]
    #                                         .shape[1:]))
    #                          .rechunk((max_first_dim_steps,) +
    #                                   dataset.variables[var_name].shape[1:]))
    #
    #    da.store(source, dest, region=setitem_tuple)
    # return output

        num_frst_dim_chk = int(np.ceil(dataset.variables[var_name].shape[0] /
                               float(max_first_dim_steps)))
        for frst_dim_chk in range(num_frst_dim_chk):
            first_dim_slice = slice(frst_dim_chk*max_first_dim_steps,
                                    min((frst_dim_chk+1)*max_first_dim_steps,
                                        dataset.variables[var_name].shape[0]),
                                    1)
            output = append_dataset_first_dim_slice(dataset, output,
                                                    var_name, first_dim_slice,
                                                    record_dimensions,
                                                    check_empty)
    return output


def append_dataset_first_dim_slice(dataset, output, var_name, first_dim_slice,
                                   record_dimensions, check_empty):
    # Create a setitem tuple
    setitem_list = [slice(0, _dim_len(dataset, dim), 1)
                    if dim not in record_dimensions
                    else record_dimensions[dim]['append_slice']
                    for dim in dataset.variables[var_name].dimensions]

    # Pick a first_dim_slice along the first dimension:
    setitem_list[0] = indices_utils.slice_a_slice(setitem_list[0],
                                                  first_dim_slice)
    temp = dataset.variables[var_name][first_dim_slice, ...]
    assign_not_masked(temp, output.variables[var_name],
                      setitem_list, check_empty)
    return output


def assign_not_masked(source, dest, setitem_list, check_empty):
    # Assign only if not masked everywhere:
    if (not hasattr(source, 'mask') or
        not check_empty or
       not source.mask.all()):

        try:
            dest[tuple(setitem_list)] = np.ma.filled(source)
        except AttributeError as e:
            errors_to_ignore = ["'str' object has no attribute 'size'",
                                "'unicode' object has no attribute 'size'"]
            if (str(e) in errors_to_ignore and
               len(setitem_list) == 1):
                for source_id, dest_id in enumerate(setitem_list[0]):
                    dest[dest_id] = source[source_id]
            else:
                raise
    return


@default
def replicate_and_copy_variable(dataset, output, var_name,
                                datatype=None, fill_value=None,
                                add_dim=None,
                                chunksize=None, zlib=False,
                                transform=(lambda x, y, z: y),
                                slices=dict(),
                                check_empty=False):

    if not isinstance(slices, dict):
        # Assume it is a function that takes the dataset as input and outputs
        # a slicing dict
        comp_slices = slices(dataset)
    else:
        comp_slices = slices

    replicate_netcdf_var(dataset, output, var_name,
                         datatype=datatype, fill_value=fill_value,
                         add_dim=add_dim,
                         slices=comp_slices,
                         chunksize=chunksize, zlib=zlib)

    # Apply a transformation if dimensions are in slices:
    if (set(comp_slices.keys())
       .issubset(dataset.variables[var_name].dimensions)):
        transform(dataset, output, comp_slices)

    if len(dataset.variables[var_name].dimensions) == 0:
        # Scalar variable:
        value = dataset.variables[var_name][...]
        if not np.ma.is_masked(value):
            # If not masked, assign. Otherwise, do nothing
            try:
                output.variables[var_name][...] = value
            except AttributeError as e:
                # This appears to be a netcdf4 bug. Skip this error at moment.
                if not (str(e) == "type object 'str' has "
                                  "no attribute 'kind'" and
                        value == ''):
                    raise
        return output

    variable_size = min(dataset.variables[var_name].shape)
    storage_size = variable_size
    if hasattr(dataset, '_h5ds'):
        # Use the hdf5 library to find the real size of the stored array:
        variable_size = dataset.variables[var_name]._h5ds.size
        storage_size = dataset.variables[var_name]._h5ds.id.get_storage_size()

    if variable_size > 0 and storage_size > 0:
        max_request = 450.0  # Maximum request in Mb

        # Create the output variable shape, allowing slices:
        var_shape = tuple([dataset.variables[var_name].shape[dim_id]
                           if dim not in comp_slices
                           else len(np.arange(dataset
                                              .variables[var_name]
                                              .shape[dim_id])
                                    [comp_slices[dim]])
                           for dim_id, dim in enumerate(dataset
                                                        .variables[var_name]
                                                        .dimensions)])
        max_first_dim_steps = max(int(np.floor(max_request*1024*1024 /
                                      (32*np.prod(var_shape[1:])))),
                                  1)

#    # Using dask. Not working yet:
#        getitem_tuple = tuple([comp_slices[var_dim]
#                               if var_dim in comp_slices.keys()
#                               else slice(None,None,1) for var_dim in
#                               dataset.variables[var_name].dimensions])
#        source = ( da.from_array(dataset.variables[var_name],
#                                 chunks=(1,) + (dataset
#                                                .variables[var_name]
#                                                .shape[1:]))[getitem_tuple]
#                               .rechunk((max_first_dim_steps,) +
#                                        output.variables[var_name].shape[1:]))
#        dest = ( da.from_array(output.variables[var_name],
#                               chunks=(max_first_dim_steps,) + (output
#                                                                .variables[var_name]
#                                                                .shape[1:])))
#
#        da.store(source, dest)
#    return output
        num_frst_dim_chk = int(np.ceil(var_shape[0] /
                               float(max_first_dim_steps)))

        for frst_dim_chk in range(num_frst_dim_chk):
            first_dim_slice = slice(frst_dim_chk*max_first_dim_steps,
                                    min((frst_dim_chk + 1)*max_first_dim_steps,
                                        var_shape[0]), 1)
            output = copy_dataset_first_dim_slice(dataset, output, var_name,
                                                  first_dim_slice,
                                                  check_empty,
                                                  slices=comp_slices)
    return output


def copy_dataset_first_dim_slice(dataset, output, var_name, first_dim_slice,
                                 check_empty, slices=dict()):
    comb_slices = slices.copy()
    first_dim = dataset.variables[var_name].dimensions[0]
    if first_dim in comb_slices:
        comb_slices[first_dim] = (indices_utils
                                  .slice_a_slice(comb_slices[first_dim],
                                                 first_dim_slice))
    else:
        comb_slices[first_dim] = first_dim_slice

    getitem_tuple = tuple([comb_slices[var_dim]
                           if var_dim in comb_slices
                           else slice(None, None, 1) for var_dim in
                           dataset.variables[var_name].dimensions])

    temp = dataset.variables[var_name][getitem_tuple]
    assign_not_masked(temp, output.variables[var_name],
                      [first_dim_slice, Ellipsis], check_empty)
    return output


@default
def replicate_group(dataset, output, group_name):
    output_grp = create_group(dataset, output, group_name)
    replicate_netcdf_file(dataset.groups[group_name], output_grp)
    return output_grp


@default
def create_group(dataset, output, group_name):
    if group_name not in output.groups:
        output_grp = output.createGroup(group_name)
    else:
        output_grp = output.groups[group_name]
    return output_grp


@default
def replicate_netcdf_file(dataset, output):
    for att in dataset.ncattrs():
        # Use np.asscalar(np.asarray()) for backward and
        # forward compatibility:
        att_val = getncattr(dataset, att)

        # This fix is for compatitbility with h5netcdf:
        if ('dtype' in dir(att_val) and
           att_val.dtype == np.dtype('O')):
            if len(att_val) == 1:
                att_val = att_val[0]
            else:
                att_val = np.asarray(att_val, dtype='str')

        if 'encode' in dir(att_val):
            try:
                att_val = str(att_val.encode('ascii', 'replace'))
            except UnicodeDecodeError:
                att_val = str(att_val)

        if (att not in output.ncattrs() and
           att != 'cdb_query_temp'):
            setncattr(output, att, att_val)
    return output


def setncattr(output, att, att_val):
    try:
        output.setncattr_string(att, att_val)
    except AttributeError:
        output.setncattr(att, att_val)
    return


def getncattr(dataset, att):
    return _toscalar(np.asarray(dataset.getncattr(att)))


def _is_dimension_present(dataset, dim):
    if dim in dataset.dimensions:
        return True
    elif dataset.parent is not None:
        return _is_dimension_present(dataset.parent, dim)
    else:
        return False


@default
def replicate_netcdf_var_dimensions(dataset, output, var,
                                    slices=dict(),
                                    datatype=None,
                                    fill_value=None,
                                    add_dim=None,
                                    chunksize=None, zlib=False):
    for dims in dataset.variables[var].dimensions:
        if (not _is_dimension_present(output, dims) and
           _is_dimension_present(dataset, dims)):
            if _isunlimited(dataset, dims):
                output.createDimension(dims, None)
            elif dims in slices:
                output.createDimension(dims,
                                       len(np.arange(_dim_len(dataset, dims))
                                           [slices[dims]]))
            else:
                output.createDimension(dims, _dim_len(dataset, dims))
            if dims in dataset.variables:
                replicate_netcdf_var(dataset, output, dims,
                                     zlib=True, slices=slices)
                if dims in slices:
                    output.variables[dims][:] = (dataset
                                                 .variables[dims]
                                                 [slices[dims]])
                else:
                    output.variables[dims][:] = dataset.variables[dims][:]
                if ('bounds' in output.variables[dims].ncattrs() and
                    getncattr(output.variables[dims], 'bounds')
                   in dataset.variables):
                    var_bounds = getncattr(output.variables[dims], 'bounds')
                    if var_bounds not in output.variables:
                        output = replicate_netcdf_var(dataset, output,
                                                      var_bounds, zlib=True,
                                                      slices=slices)
                        if dims in slices:
                            getitem_tuple = tuple([slices[var_bounds_dim]
                                                   if var_bounds_dim
                                                   in slices
                                                   else slice(None, None, 1)
                                                   for var_bounds_dim in
                                                   (dataset
                                                    .variables[var_bounds]
                                                    .dimensions)])
                            output.variables[var_bounds][:] = (dataset
                                                               .variables
                                                               [var_bounds]
                                                               [getitem_tuple])
                        else:
                            output.variables[var_bounds][:] = (dataset
                                                               .variables
                                                               [var_bounds][:])
            else:
                # Create a dummy dimension variable:
                dim_var = output.createVariable(dims, np.float, (dims,),
                                                chunksizes=(1,))
                if dims in slices:
                    dim_var[:] = (np.arange(_dim_len(dataset, dims))
                                  [slices[dims]])
                else:
                    dim_var[:] = np.arange(_dim_len(dataset, dims))
    return output


@default
def replicate_netcdf_other_var(dataset, output, var, time_dim):
    # Replicates all variables except specified variable:
    variables_list = [other_var
                      for other_var
                      in variables_list_with_time_dim(dataset, time_dim)
                      if other_var != var]
    for other_var in variables_list:
        output = replicate_netcdf_var(dataset, output, other_var)
    return output


@default
def replicate_netcdf_var(dataset, output, var,
                         slices=dict(),
                         datatype=None, fill_value=None,
                         add_dim=None, chunksize=None,
                         zlib=False):
    if var not in dataset.variables:
        return output

    output = replicate_netcdf_var_dimensions(dataset, output,
                                             var, slices=slices)
    if var in output.variables:
        # var is a dimension variable and does not need to be created:
        return output

    if datatype is None:
        datatype = _sanitized_datatype(dataset, var)
    if (isinstance(datatype, netCDF4.CompoundType) and
       datatype.name not in output.cmptypes):
        datatype = output.createCompoundType(datatype.dtype, datatype.name)

    # Weird fix for strings:
    # if 'str' in dir(datatype) and 'S1' in datatype.str:
    #     datatype='S1'

    kwargs = dict()
    if (fill_value is None and
        '_FillValue' in dataset.variables[var].ncattrs() and
       datatype == _sanitized_datatype(dataset, var)):
        kwargs['fill_value'] = getncattr(dataset.variables[var], '_FillValue')
    else:
        kwargs['fill_value'] = fill_value

    if not zlib:
        if dataset.variables[var].filters() is None:
            kwargs['zlib'] = False
        else:
            for item in dataset.variables[var].filters():
                kwargs[item] = dataset.variables[var].filters()[item]
    else:
        kwargs['zlib'] = zlib

    if var not in output.variables:
        dimensions = dataset.variables[var].dimensions
        time_dim = find_time_dim(dataset)
        if add_dim:
            dimensions += (add_dim,)
        var_shape = tuple([dataset.variables[var].shape[dim_id]
                           if dim not in slices
                           else len(np.arange(dataset
                                              .variables[var]
                                              .shape[dim_id])[slices[dim]])
                           for dim_id, dim in enumerate(dimensions)])
        if chunksize == -1:
            chunksizes = tuple([1 if dim == time_dim
                                else var_shape[dim_id]
                                for dim_id, dim in enumerate(dimensions)])
        elif dataset.variables[var].chunking() == 'contiguous':
            if kwargs['zlib']:
                chunksizes = tuple([1 if dim == time_dim
                                    else var_shape[dim_id]
                                    for dim_id, dim in enumerate(dimensions)])
            else:
                chunksizes = tuple([1 for dim_id, dim
                                    in enumerate(dimensions)])
        else:
            if len(set(dimensions).intersection(slices.keys())) > 0:
                if kwargs['zlib']:
                    chunksizes = tuple([1 if dim == time_dim
                                        else var_shape[dim_id]
                                        for dim_id, dim
                                        in enumerate(dimensions)])
                else:
                    chunksizes = tuple([1 for dim_id, dim
                                        in enumerate(dimensions)])
            else:
                chunksizes = dataset.variables[var].chunking()
        kwargs['chunksizes'] = chunksizes
        output.createVariable(var, datatype, dimensions, **kwargs)
    output = replicate_netcdf_var_att(dataset, output, var)
    return output


def _toscalar(x):
    try:
        return np.asscalar(x)
    except (AttributeError, ValueError):
        return x


@default
def replicate_netcdf_var_att(dataset, output, var):
    for att in dataset.variables[var].ncattrs():
        # Use np.asscalar(np.asarray()) for backward and forward compatibility:
        att_val = getncattr(dataset.variables[var], att)
        if isinstance(att_val, dict):
            atts_pairs = [(att+'.' + key, att_val[key])
                          for key in att_val]
        else:
            atts_pairs = [(att, att_val)]
        for att_pair in atts_pairs:
            if att_pair[0][0] != '_':
                if 'encode' in dir(att_pair[1]):
                    att_val = att_pair[1].encode('ascii', 'replace')
                else:
                    att_val = att_pair[1]
                if 'encode' in dir(att_pair[0]):
                    att = att_pair[0].encode('ascii', 'replace')
                else:
                    att = att_pair[0]
                setncattr(output.variables[var], att, att_val)
    return output


@default
def create_time_axis(dataset, output, time_axis,
                     time_var='time'):
    time_dim = find_time_dim(dataset, time_var=time_var)
    output.createDimension(time_dim, None)
    time = output.createVariable(time_dim, 'd', (time_dim,),
                                 chunksizes=(1,))
    if dataset is None:
        setncattr(time, 'calendar', 'standard')
        setncattr(time, 'units', 'days since '+str(time_axis[0]))
    else:
        setncattr(time, 'calendar',
                  netcdf_calendar(dataset, time_var=time_var))
        time_var = find_time_var(dataset, time_var=time_var)
        # Use np.asscalar(np.asarray()) for backward and forward compatibility:
        setncattr(time, 'units',
                  str(getncattr(dataset.variables[time_var], 'units')))
    time[:] = time_axis
    return output


def create_time_axis_date(output, time_axis, units, calendar, time_dim='time'):
    output.createDimension(time_dim, None)
    time = output.createVariable(time_dim, 'd', (time_dim,), chunksizes=(1,))
    setncattr(time, 'calendar', calendar)
    setncattr(time, 'units', units)
    time[:] = get_time_axis_relative(time_axis, getncattr(time, 'units'),
                                     getncattr(time, 'calendar'))
    return


@default
def netcdf_calendar(dataset, time_var='time'):
    calendar = 'standard'

    time_var = find_time_var(dataset, time_var=time_var)
    if time_var is not None:
        if 'calendar' in dataset.variables[time_var].ncattrs():
            # Use np.asscalar(np.asarray()) for backward and
            # forward compatibility:
            calendar = getncattr(dataset.variables[time_var], 'calendar')
        if 'encode' in dir(calendar):
            calendar = calendar.encode('ascii', 'replace')
    return calendar


@default
def find_time_var(dataset, time_var='time'):
    var_list = dataset.variables.keys()
    return find_time_name_from_list(var_list, time_var)


@default
def find_time_dim(dataset, time_var='time'):
    dim_list = dataset.dimensions.keys()
    return find_time_name_from_list(dim_list, time_var)


def find_time_name_from_list(list_of_names, time_var):
    try:
        return list_of_names[next(i for i, v in enumerate(list_of_names)
                             if v.lower() == time_var)]
    except StopIteration:
        return None


@default
def variables_list_with_time_dim(dataset, time_dim):
    return [var for var in dataset.variables
            if time_dim in dataset.variables[var].dimensions]


@default
def find_dimension_type(dataset, time_var='time'):
    dimension_type = OrderedDict()

    time_dim = find_time_name_from_list(dataset.dimensions.keys(), time_var)
    for dim in dataset.dimensions:
        if dim != time_dim:
            dimension_type[dim] = _dim_len(dataset, dim)
    return dimension_type


@default
def netcdf_time_units(dataset, time_var='time'):
    units = None
    time_var = find_time_var(dataset, time_var=time_var)
    if 'units' in dataset.variables[time_var].ncattrs():
        # Use np.asscalar(np.asarray()) for backward and forward compatibility:
        units = getncattr(dataset.variables[time_var], 'units')
    return units


@default
def retrieve_dimension(dataset, dimension):
    attributes = dict()

    if dimension in dataset.variables:
        # Retrieve attributes:
        for att in dataset.variables[dimension].ncattrs():
            # Use np.asscalar(np.asarray()) for backward and
            # forward compatibility:
            attributes[att] = getncattr(dataset.variables[dimension], att)
        # If dimension is available, retrieve
        dimension_dataset = dataset.variables[dimension][...]
    else:
        # If dimension is not avaiable, create a simple indexing dimension
        dimension_dataset = np.arange(_dim_len(dataset, dimension))
    return dimension_dataset, attributes


@default
def retrieve_dimension_list(dataset, var):
    return dataset.variables[var].dimensions


def retrieve_dimensions_no_time(dataset, var, time_var='time'):
    dimensions_data = dict()
    attributes = dict()
    dimensions = retrieve_dimension_list(dataset, var)
    time_dim = find_time_name_from_list(dimensions, time_var)
    for dim in dimensions:
        if dim != time_dim:
            dimensions_data[dim], attributes[dim] = retrieve_dimension(dataset,
                                                                       dim)
    return dimensions_data, attributes


@default
def retrieve_variables(dataset, output, zlib=True):
    for var_name in dataset.variables:
        output = replicate_and_copy_variable(dataset, output, var_name,
                                             zlib=zlib, check_empty=False)
    return output


@default
def retrieve_variables_no_time(dataset, output, time_dim, zlib=False):
    for var in dataset.variables:
        if ((time_dim not in dataset.variables[var].dimensions) and
           (var not in output.variables)):
            replicate_and_copy_variable(dataset, output, var, zlib=zlib)
    return output


@default
def find_time_dim_and_replicate_netcdf_file(dataset, output, time_var='time'):
    return (find_time_dim(dataset, time_var=time_var),
            replicate_netcdf_file(dataset, output))


@default
def create_date_axis_from_time_axis(time_axis, attributes_dict):
    calendar = 'standard'
    units = attributes_dict['units']
    if 'calendar' in attributes_dict:
        calendar = attributes_dict['calendar']

    if units == 'day as %Y%m%d.%f':
        date_axis = np.array(map(convert_to_date_absolute,
                                 time_axis))
    else:
        try:
            date_axis = get_date_axis_relative(time_axis, units, calendar)
        except TypeError:
            date_axis = np.array([])
    return date_axis


@default
def retrieve_container(dataset, var, dimensions, unsort_dimensions,
                       sort_table, max_request, time_var='time',
                       file_name=''):
    remote_dims, attributes = retrieve_dimensions_no_time(dataset, var,
                                                          time_var=time_var)

    idx = copy.copy(dimensions)
    unsort_idx = copy.copy(unsort_dimensions)
    for dim in remote_dims:
        idx[dim], unsort_idx[dim] = (indices_utils
                                     .prepare_indices(indices_utils
                                                      .get_indices_from_dim(
                                                              remote_dims[dim],
                                                              idx[dim])))
    return grab_indices(dataset, var, idx, unsort_idx,
                        max_request, file_name=file_name)


@default
def grab_indices(dataset, var, indices, unsort_indices, max_request,
                 file_name=''):
    dimensions = retrieve_dimension_list(dataset, var)
    return indices_utils.retrieve_slice(dataset.variables[var], indices,
                                        unsort_indices,
                                        dimensions[0], dimensions[1:],
                                        0, max_request)
