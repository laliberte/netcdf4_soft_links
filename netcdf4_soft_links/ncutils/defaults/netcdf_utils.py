# External:
from collections import OrderedDict
import numpy as np


def check_if_opens(*args, **kwargs):
    return False


def get_year_axis(*args, **kwargs):
    return np.array([]), np.array([])


def get_date_axis(*args, **kwargs):
    return np.array([])


def get_date_axis_from_units_and_calendar(*args, **kwargs):
    return np.array([])


def get_date_axis_relative(*args, **kwargs):
    return np.array([])


def get_date_axis_absolute(*args, **kwargs):
    return np.array([])


def get_time(*args, **kwargs):
    return np.array([])


def get_time_axis_relative(*args, **kwargs):
    return np.array([])


def replicate_full_netcdf_recursive(*args, **kwargs):
    return args[1]


def dimension_compatibility(*args, **kwargs):
    return False


def check_dimensions_compatibility(*args, **kwargs):
    return False


def append_record(*args, **kwargs):
    return dict()


def ensure_compatible_time_units(*args, **kwargs):
    return args[0].variables[args[2]][:]


def append_and_copy_variable(*args, **kwargs):
    return args[1]


def replicate_and_copy_variable(*args, **kwargs):
    return args[1]


def replicate_group(*args, **kwargs):
    return args[1]


def create_group(*args, **kwargs):
    return args[1]


def replicate_netcdf_file(*args, **kwargs):
    return args[1]


def replicate_netcdf_var_dimensions(*args, **kwargs):
    return args[1]


def replicate_netcdf_other_var(*args, **kwargs):
    return args[1]


def replicate_netcdf_var(*args, **kwargs):
    # Create empty variable:
    args[1].createVariable(args[2], 'd', ())
    return args[1]


def replicate_netcdf_var_att(*args, **kwargs):
    return args[1]


def create_time_axis(*args, **kwargs):
    return args[1]


def netcdf_calendar(*args, **kwargs):
    return 'standard'


def find_time_var(*args, **kwargs):
    if 'time_var' in kwargs:
        return kwargs['time_var']
    else:
        return 'time'


def find_time_dim(*args, **kwargs):
    if 'time_var' in kwargs:
        return kwargs['time_var']
    else:
        return 'time'


def variables_list_with_time_dim(*args, **kwargs):
    return []


def find_dimension_type(*args, **kwargs):
    return OrderedDict()


def netcdf_time_units(*args, **kwargs):
    return None


def retrieve_dimension(*args, **kwargs):
    attributes = dict()
    dimension_dataset = np.array([])
    return dimension_dataset, attributes


def retrieve_dimension_list(*args, **kwargs):
    dimensions = tuple()
    return dimensions


def retrieve_dimensions_no_time(*args, **kwargs):
    dimensions_data = dict()
    attributes = dict()
    return dimensions_data, attributes


def retrieve_variables(*args, **kwargs):
    return args[1]


def retrieve_variables_no_time(*args, **kwargs):
    return args[1]


def find_time_dim_and_replicate_netcdf_file(*args, **kwargs):
    return (find_time_dim(*args, **kwargs),
            replicate_netcdf_file(*args, **kwargs))


def create_date_axis_from_time_axis(*args, **kwargs):
    return np.array([])


def retrieve_container(*args, **kwargs):
    return np.array([])


def grab_indices(*args, **kwargs):
    return np.array([])
