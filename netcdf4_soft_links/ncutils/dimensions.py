# External:
import numpy as np
from collections import OrderedDict

# Internal:
from .core import default, find_time_name_from_list, getncattr
from .defaults import dimensions as ncu_defaults
from .dataset_compat import (_isunlimited, _dim_len)


@default(mod=ncu_defaults)
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


@default(mod=ncu_defaults)
def dimension_compatibility(dataset, output, dim):

    if (dim in output.dimensions and
       _dim_len(output, dim) != _dim_len(dataset, dim)):
        # Dimensions mismatch, return without writing anything
        return False  # pragma: no cover, will always work
    elif ((dim in dataset.variables and
           dim in output.variables) and
          (len(output.variables[dim]) != len(dataset.variables[dim]) or
           (dataset.variables[dim][:] != dataset.variables[dim][:]).any())):
        # Dimensions variables mismatch, return without writing anything
        return False  # pragma: no cover, will always work
    else:
        return True  # pragma: no cover, will always work


@default(mod=ncu_defaults)
def check_dimensions_compatibility(dataset, output, var_name,
                                   exclude_unlimited=False):
    for dim in dataset.variables[var_name].dimensions:
        # The dimensions might be in the parent group:
        dataset_parent = get_dataset_with_dimension(dataset, dim)
        output_parent = get_dataset_with_dimension(output, dim)

        if ((not _isunlimited(dataset_parent, dim) or
             not exclude_unlimited) and
           not dimension_compatibility(dataset_parent, output_parent, dim)):
            return False  # pragma: no cover, this will always work.
    return True


def get_dataset_with_dimension(dataset, dim):
    if (dim not in dataset.dimensions or
       dim not in dataset.variables):
        dataset_parent = dataset.parent
    else:
        dataset_parent = dataset
    return dataset_parent


def _is_dimension_present(dataset, dim):
    if dim in dataset.dimensions:
        return True
    elif dataset.parent is not None:
        return _is_dimension_present(dataset.parent, dim)
    else:
        return False


@default(mod=ncu_defaults)
def find_dimension_type(dataset, time_var='time'):
    dimension_type = OrderedDict()

    time_dim = find_time_name_from_list(dataset.dimensions.keys(), time_var)
    for dim in dataset.dimensions:
        if dim != time_dim:
            dimension_type[dim] = _dim_len(dataset, dim)
    return dimension_type
