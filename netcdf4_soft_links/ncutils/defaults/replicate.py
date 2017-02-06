from .time import find_time_dim


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


def find_time_dim_and_replicate_netcdf_file(*args, **kwargs):
    return (find_time_dim(*args, **kwargs),
            replicate_netcdf_file(*args, **kwargs))
