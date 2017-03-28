# External:
import copy

# Internal:
from .indices import (prepare_indices, get_indices_from_dim,
                      retrieve_slice)
from .core import default, find_time_name_from_list
from .replicate import replicate_and_copy_variable
from .defaults import retrieval as ncu_defaults
from .dimensions import retrieve_dimension


@default(mod=ncu_defaults)
def retrieve_container(dataset, var, dimensions, unsort_dimensions,
                       sort_table, max_request, time_var='time',
                       file_name=''):
    # print(dataset, var, dimensions, unsort_dimensions, sort_table)
    remote_dims, attributes = retrieve_dimensions_no_time(dataset, var,
                                                          time_var=time_var)

    idx = copy.copy(dimensions)
    unsort_idx = copy.copy(unsort_dimensions)
    for dim in remote_dims:
        idx[dim], unsort_idx[dim] = prepare_indices(
                                        get_indices_from_dim(remote_dims[dim],
                                                             idx[dim], dim=dim))
    return grab_indices(dataset, var, idx, unsort_idx,
                        max_request, file_name=file_name)


@default(mod=ncu_defaults)
def grab_indices(dataset, var, indices, unsort_indices, max_request,
                 file_name=''):
    dimensions = retrieve_dimension_list(dataset, var)
    return retrieve_slice(dataset.variables[var], indices,
                          unsort_indices, dimensions[0], dimensions[1:],
                          0, max_request)


@default(mod=ncu_defaults)
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


@default(mod=ncu_defaults)
def retrieve_variables(dataset, output, zlib=True):
    for var_name in dataset.variables:
        output = replicate_and_copy_variable(dataset, output, var_name,
                                             zlib=zlib, check_empty=False)
    return output


@default(mod=ncu_defaults)
def retrieve_variables_no_time(dataset, output, time_dim='time', zlib=False):
    for var in dataset.variables:
        if ((time_dim not in dataset.variables[var].dimensions) and
           (var not in output.variables)):
            replicate_and_copy_variable(dataset, output, var, zlib=zlib)
    return output
