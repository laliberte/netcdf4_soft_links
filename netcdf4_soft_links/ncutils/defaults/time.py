import numpy as np


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


def ensure_compatible_time_units(*args, **kwargs):
    return args[0].variables[args[2]][:]


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


def netcdf_time_units(*args, **kwargs):
    return None


def create_date_axis_from_time_axis(*args, **kwargs):
    return np.array([])
