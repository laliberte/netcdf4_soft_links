"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from .dataset_compat import _dim_len
from .core import getncattr, maybe_conv_bytes_to_str_array


def check_netcdf_equal(dataset, output, slices=dict()):
    assert check_att_equal(dataset, output)
    for dim in dataset.dimensions:
        assert check_dim_equal(dataset, output, dim, slices=slices)

    for var in dataset.variables:
        assert check_var_equal(dataset, output, var, slices=slices)

    for group in dataset.groups:
        assert group in output.groups
        assert check_netcdf_equal(dataset.groups[group], output.groups[group],
                                  slices=slices)
    return True


def check_att_equal(dataset, output):
    for att in dataset.ncattrs():
        assert att in output.ncattrs()
        try:
            assert (getncattr(dataset, att) ==
                    getncattr(output, att))
        except ValueError:
            assert (getncattr(dataset, att) ==
                    getncattr(output, att)).all()
    return True


def check_dim_equal(dataset, output, dim, slices=dict):
    assert dim in output.dimensions
    if dim not in slices:
        try:
            assert _dim_len(dataset, dim) == _dim_len(output, dim)
        except AssertionError:
            print(dataset, output)
            raise
    else:
        assert (len(range(_dim_len(dataset, dim))[slices[dim]]) ==
                _dim_len(output, dim))
    # This is special for orphan dimensions:
    if (dim not in dataset.variables and
       dim in output.variables):
        if dim not in slices:
            np.testing.assert_equal(output.variables[dim][:],
                                    range(_dim_len(dataset, dim)))
        else:
            np.testing.assert_equal(output.variables[dim][:],
                                    range(_dim_len(dataset, dim))
                                    [slices[dim]])
    return True


def check_var_equal(dataset, output, var, slices=dict()):
    assert var in output.variables
    for dim1, dim2 in zip(dataset.variables[var].dimensions,
                          output.variables[var].dimensions):
        assert dim1 == dim2
    key = tuple([slice(None) if dim not in slices
                 else slices[dim]
                 for dim in dataset.variables[var].dimensions])
    np.testing.assert_equal(
     np.ma.filled(maybe_conv_bytes_to_str_array(dataset.variables[var][key])),
     np.ma.filled(maybe_conv_bytes_to_str_array(output.variables[var][key])))
    assert check_att_equal(dataset.variables[var],
                           output.variables[var])
    return True
