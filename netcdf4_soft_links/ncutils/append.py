# External:
import numpy as np

# Internal:
from .indices import slice_a_slice
from .core import default, DEFAULT_MAX_REQUEST
from .time import ensure_compatible_time_units
from .replicate import WrapperSetItem, storage_chunks
from .defaults import append as ncu_defaults
from .dataset_compat import (_isunlimited, _dim_len)
try:
    import dask.array as da
    import dask
    with_dask = True
except ImportError:
    with_dask = False


@default(mod=ncu_defaults)
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


@default(mod=ncu_defaults)
def append_and_copy_variable(dataset, output, var_name, record_dimensions,
                             check_empty=False, allow_dask=False):

    if len(set(record_dimensions.keys())
           .intersection(dataset.variables[var_name].dimensions)) == 0:

        # Variable does not contain a record dimension, return
        return output

    if hasattr(dataset.variables[var_name], '_h5ds'):
        # Use the hdf5 library to find the real size of the stored array:
        variable_size = dataset.variables[var_name]._h5ds.size
        storage_size = dataset.variables[var_name]._h5ds.id.get_storage_size()
    else:
        variable_size = min(dataset.variables[var_name].shape)
        storage_size = variable_size

    if variable_size > 0 and storage_size > 0:
        if with_dask and allow_dask:
            output = incremental_setitem_with_dask(
                                            dataset, output, var_name,
                                            check_empty, record_dimensions)
        else:
            output = incremental_setitem_without_dask(
                                            dataset, output, var_name,
                                            check_empty, record_dimensions)
    return output


def incremental_setitem_with_dask(dataset, output, var_name, check_empty,
                                  record_dimensions):
    source = da.from_array(dataset.variables[var_name],
                           chunks=storage_chunks(dataset.variables[var_name]))
    dest = WrapperSetItem(output.variables[var_name])

    setitem_tuple = tuple([slice(0, _dim_len(dataset, dim), 1)
                           if dim not in record_dimensions
                           else record_dimensions[dim]['append_slice']
                           for dim
                           in dataset.variables[var_name].dimensions])

    with dask.set_options(get=dask.async.get_sync):
        da.store(source, dest, region=setitem_tuple)
    return output


def incremental_setitem_without_dask(dataset, output, var_name, check_empty,
                                     record_dimensions):
    var_shape = dataset.variables[var_name].shape[1:]
    max_request = DEFAULT_MAX_REQUEST  # maximum request in Mb
    max_first_dim_steps = max(int(np.floor(max_request*1024*1024 /
                                           (32*np.prod(var_shape)))), 1)
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
    setitem_list[0] = slice_a_slice(setitem_list[0], first_dim_slice)
    source = dataset.variables[var_name][first_dim_slice, ...]
    dest = WrapperSetItem(output.variables[var_name], check_empty)
    dest[setitem_list] = source
    return output
