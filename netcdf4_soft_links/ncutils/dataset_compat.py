# External:
import numpy as np
import h5netcdf.legacyapi as netCDF4_h5


def _isunlimited(dataset, dim):
    if (isinstance(dataset, netCDF4_h5.Dataset) or
       isinstance(dataset, netCDF4_h5.Group)):
        var_list_with_dim = [var for var in dataset.variables
                             if dim in dataset.variables[var].dimensions]
        if len(var_list_with_dim) == 0:
            raise KeyError(('Dimension {0} is not associated with '
                            'any variable').format(dim))

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
        if dim in dataset.dimensions:
            return len(dataset.dimensions[dim])
        else:
            return _dim_len(dataset.parent, dim)


def _shape(dataset, var):
    return tuple([_dim_len(dataset, dim) for dim
                  in dataset.variables[var].dimensions])


def _sanitized_datatype(dataset, var):
    try:
        datatype = dataset.variables[var].datatype
    except KeyError:
        datatype = dataset.variables[var].dtype
    if isinstance(datatype, np.dtype):
        if datatype.char == 'O':
            # Object datatypes are assumed to be strings:
            return np.str
        else:
            try:
                return np.dtype(datatype.name)
            except TypeError:
                if datatype.kind in ['S', 'U']:
                    return np.str
                else:  # pragma: no cover
                    return datatype
    else:
        return np.dtype(datatype)
