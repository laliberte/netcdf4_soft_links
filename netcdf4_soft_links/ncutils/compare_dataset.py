"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from .dataset_compat import _dim_len
from .core import getncattr


def check_netcdf_equal(dataset, output, slices=dict()):
    assert check_att_equal(dataset, output)
    for dim in dataset.dimensions:
        assert dim in output.dimensions
        if dim not in slices:
            assert _dim_len(dataset, dim) == _dim_len(output, dim)
        else:
            assert (len(range(_dim_len(dataset, dim))[slices[dim]]) ==
                    _dim_len(output, dim))
        # This is special for orphan dimensions:
        if dim not in dataset.variables:
            assert dim in output.variables
            if dim not in slices:
                np.testing.assert_equal(output.variables[dim][:],
                                        range(_dim_len(dataset, dim)))
            else:
                np.testing.assert_equal(output.variables[dim][:],
                                        range(_dim_len(dataset, dim))
                                        [slices[dim]])
    for var in dataset.variables:
        assert var in output.variables
        for dim1, dim2 in zip(dataset.variables[var].dimensions,
                              output.variables[var].dimensions):
            assert dim1 == dim2
        key = tuple([slice(None) if dim not in slices
                     else slices[dim]
                     for dim in dataset.variables[var].dimensions])
        np.testing.assert_equal(dataset.variables[var][key],
                                output.variables[var][key])
        assert check_att_equal(dataset.variables[var],
                               output.variables[var])
    for group in dataset.groups:
        assert group in output.groups
        check_netcdf_equal(dataset.groups[group],
                           output.groups[group],
                           slices=slices)
    return True


def check_att_equal(dataset, output):
    for att in dataset.ncattrs():
        print(dataset, output)
        assert att in output.ncattrs()
        try:
            assert (getncattr(dataset, att) ==
                    getncattr(output, att))
        except ValueError:
            assert (getncattr(dataset, att) ==
                    getncattr(output, att)).all()
    return True
