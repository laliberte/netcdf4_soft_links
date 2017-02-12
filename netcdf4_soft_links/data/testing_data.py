"""
Generate test data for netcdf4_soft_links
"""

import numpy as np
from collections import OrderedDict

from netCDF4 import Dataset


def create_data():
    struc_dtype = [('temperature', np.dtype('float32')),
                   ('flag', np.dtype('S3')),
                   ('number', np.dtype('int32'))]
    data_dict = {'temperature': np.arange(260, 315) + 0.5,
                 'flag': ['aaa', 'aba', 'abc', 'bb', 'cab', 'bc'],
                 'number': range(10)}
    shape = (2, 2, 2, 2)
    data = np.empty(shape, dtype=struc_dtype)
    for var in data_dict:
        gen = np.random.choice(data_dict[var], size=shape)
        for index in np.ndindex(shape):
            data[var][index] = gen[index]
    return data


def create_test_file(file_name, data, path, time_offset):
    dim_values = OrderedDict([('time', None),
                              ('plev', [1e5, 1e4]),
                              ('lat', [0.0, 90.0]),
                              ('lon', [0.0, 180.0])])
    dim_bnds_values = OrderedDict([('time', None),
                                   ('plev', [[1e5, 5e4],
                                             [5e4, 5e3]]),
                                   ('lat', [[-15.0, 15.0],
                                            [45.0, 90.0]]),
                                   ('lon', [[-30.0, 30.0],
                                            [120.0, 210.0]])])

    # Create tempfile:
    with Dataset(file_name, 'w') as output:
        out_grp = output.createGroup(path)
        for dim_id, dim in enumerate(dim_values):
            if dim_values[dim] is None:
                out_grp.createDimension(dim, None)
                temp = out_grp.createVariable(dim, 'd', (dim,))
                temp[:] = np.array([0.0, 1.0]) + time_offset
                temp.setncattr_string('calendar', 'standard')
                temp.setncattr_string('units', 'days since 1980-01-01')
                alt = out_grp.createVariable(dim + '_abs', 'd', (dim,))
                alt[:] = np.array([19800101.0, 19800102.0]) + time_offset
                alt.setncattr_string('calendar', 'standard')
                alt.setncattr_string('units', 'day as %Y%m%d.%f')
            else:
                out_grp.createDimension(dim, data.shape[dim_id])
                temp = out_grp.createVariable(dim, 'd', (dim,))
                temp[:] = np.linspace(*(dim_values[dim] +
                                        [data.shape[dim_id]]))
            temp.setncattr_string('bounds', dim + '_bnds')
            if 'bnds' not in out_grp.dimensions:
                out_grp.createDimension('bnds', 2)
                bnds = out_grp.createVariable('bnds', 'f',
                                              ('bnds',))
                for val_id, val in enumerate([0, 1]):
                    bnds[val_id] = val
            dim_bnds = out_grp.createVariable(dim + '_bnds', 'd',
                                              (dim, 'bnds'))
            if dim_bnds_values[dim] is None:
                dim_bnds[:] = [temp[:]*0.95, temp[:]*1.05]
            else:
                dim_bnds[:] = np.array(dim_bnds_values[dim])

        fill_value = 1e20
        for var in data.dtype.names:
            if data[var].dtype.kind in ['U', 'S']:
                datatype = np.str
            else:
                datatype = data[var].dtype
                chunksizes = 'contiguous'
            if datatype == np.str:
                temp = out_grp.createVariable(var, datatype,
                                              tuple(dim_values.keys()))
            else:
                chunksizes = (1,) + data[var].shape[1:]
                temp = out_grp.createVariable(var, datatype,
                                              tuple(dim_values.keys()),
                                              zlib=True,
                                              chunksizes=chunksizes,
                                              fletcher32=True)
            try:
                dtype_fill_value = np.array([fill_value]).astype(temp.dtype)
            except (TypeError, AttributeError):
                pass
            if isinstance(dtype_fill_value, np.floating):
                temp.setncattr('_FillValue', dtype_fill_value)

            if datatype == np.str:
                for index in np.ndindex(temp.shape):
                    if hasattr(data[var][index], 'decode'):
                        temp[index] = np.str(data[var][index]
                                             .decode('ascii'))
            else:
                temp[:] = data[var]
            if temp.chunking() == 'contiguous':
                temp.setncattr_string('chunksizes', 'contiguous')
            else:
                temp.setncattr('chunksizes', temp.chunking())
            temp.setncattr_string('short_name', var)
        out_grp.setncattr_string('history', 'test group for netcdf_utils')
        output.setncattr_string('history', 'test file for netcdf_utils')
    return


def generate_test_files(request, tmpdir):
    data_tmpdir = tmpdir.mkdir('data')
    idx = 0
    while True:
        file_name = data_tmpdir.join('test_{0}.nc'.format(idx))
        data = create_data()
        create_test_file(file_name, data, request.param, idx)
        idx += 1
        yield str(file_name), data
