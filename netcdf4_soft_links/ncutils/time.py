# External:
import numpy as np
import math
import netCDF4
import datetime

# Internal:
from .core import default, getncattr, setncattr, find_time_name_from_list
from .defaults import time as ncu_defaults
from .dimensions import retrieve_dimension


@default(mod=ncu_defaults)
def get_year_axis(dataset, time_var=None):
    if time_var is None:
        time_var = find_time_var(dataset)
    date_axis = get_date_axis(dataset, time_var)
    year_axis = np.array([date.year for date in date_axis])
    month_axis = np.array([date.month for date in date_axis])
    return year_axis, month_axis


@default(mod=ncu_defaults)
def get_date_axis(dataset, time_var):
    # Use np.asscalar(np.asarray(x)) to ensure that attributes
    # are not arrays if lenght-1
    units = getncattr(dataset.variables[time_var], 'units')
    if 'calendar' in dataset.variables[time_var].ncattrs():
        calendar = getncattr(dataset.variables[time_var], 'calendar')
    else:
        calendar = None
    return get_date_axis_from_units_and_calendar(dataset
                                                 .variables[time_var][:],
                                                 units, calendar)


@default(mod=ncu_defaults)
def get_date_axis_from_units_and_calendar(time_axis, units, calendar):
    if units == 'day as %Y%m%d.%f':
        date_axis = get_date_axis_absolute(time_axis)
    else:
        date_axis = get_date_axis_relative(time_axis, units, calendar)
    return date_axis


@default(mod=ncu_defaults)
def get_date_axis_relative(time_axis, units, calendar):
    if calendar is not None:
        date_axis = netCDF4.num2date(time_axis, units=units,
                                     calendar=calendar)
    else:
        date_axis = netCDF4.num2date(time_axis, units=units)
    return date_axis


@default(mod=ncu_defaults)
def get_date_axis_absolute(time_axis):
    return np.array([convert_to_date_absolute(x) for x in time_axis])


@default(mod=ncu_defaults)
def get_time(dataset, time_var='time'):
    time_var = find_time_var(dataset, time_var=time_var)
    time_axis, attributes = retrieve_dimension(dataset, time_var)
    date_axis = create_date_axis_from_time_axis(time_axis, attributes)
    return date_axis


@default(mod=ncu_defaults)
def get_time_axis_relative(date_axis, units, calendar):
    if calendar is not None:
        time_axis = netCDF4.date2num(date_axis, units=units,
                                     calendar=calendar)
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


@default(mod=ncu_defaults)
def netcdf_calendar(dataset, time_var='time'):
    calendar = 'standard'

    time_var = find_time_var(dataset, time_var=time_var)
    if time_var is not None:
        if 'calendar' in dataset.variables[time_var].ncattrs():
            # Use np.asscalar(np.asarray()) for backward and
            # forward compatibility:
            calendar = getncattr(dataset.variables[time_var], 'calendar')
        if hasattr(calendar, 'encode'):
            calendar = (calendar
                        .encode('ascii', 'replace')
                        .decode('ascii'))
    return calendar


@default(mod=ncu_defaults)
def find_time_var(dataset, time_var='time'):
    var_list = dataset.variables.keys()
    return find_time_name_from_list(var_list, time_var)


@default(mod=ncu_defaults)
def find_time_dim(dataset, time_var='time'):
    dim_list = dataset.dimensions.keys()
    return find_time_name_from_list(dim_list, time_var)


@default(mod=ncu_defaults)
def ensure_compatible_time_units(dataset, output, dim):
    try:
        units = dict()
        calendar = dict()
        for desc, data in [('source', dataset), ('dest', output)]:
            units[desc] = getncattr(data.variables[dim], 'units')
            if 'calendar' in data.variables[dim].ncattrs():
                calendar[desc] = getncattr(data.variables[dim], 'calendar')
            else:  # pragma: no cover
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


@default(mod=ncu_defaults)
def create_time_axis(dataset, output, time_axis,
                     time_var='time'):
    if dataset is None:
        time_dim = time_var
    else:
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
    return output


@default(mod=ncu_defaults)
def variables_list_with_time_dim(dataset, time_dim):
    return [var for var in dataset.variables
            if time_dim in dataset.variables[var].dimensions]


@default(mod=ncu_defaults)
def netcdf_time_units(dataset, time_var='time'):
    units = None
    time_var = find_time_var(dataset, time_var=time_var)
    if 'units' in dataset.variables[time_var].ncattrs():
        # Use np.asscalar(np.asarray()) for backward and forward compatibility:
        units = getncattr(dataset.variables[time_var], 'units')
    return units


@default(mod=ncu_defaults)
def create_date_axis_from_time_axis(time_axis, attributes_dict):
    calendar = 'standard'
    units = attributes_dict['units']
    if 'calendar' in attributes_dict:
        calendar = attributes_dict['calendar']

    if units == 'day as %Y%m%d.%f':
        date_axis = np.array([convert_to_date_absolute(x) for x
                              in time_axis])
    else:
        date_axis = get_date_axis_relative(time_axis, units, calendar)
    return date_axis
