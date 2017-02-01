"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from collections import OrderedDict
from contextlib import closing


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links import dataset_compat as dc

from netcdf4_soft_links.netcdf4_pydap import Dataset as pydap_Dataset
from netCDF4 import Dataset as nc4_Dataset
from h5netcdf.legacyapi import Dataset as h5_Dataset

import pytest


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
                              ('lon', [0.0, 360.0])])

    # Create tempfile:
    with nc4_Dataset(file_name, 'w') as output:
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
            dim_bnds[:] = [temp[:]*0.95, temp[:]*1.05]

        fill_value = 1e20
        for var in data.dtype.names:
            temp = out_grp.createVariable(var, data[var].dtype,
                                          tuple(dim_values.keys()),
                                          zlib=True,
                                          chunksizes=((1,) +
                                                      data[var].shape[1:]),
                                          fletcher32=True)

            try:
                temp.setncattr('_Fill_Value', (np.array([fill_value])
                                               .astype(temp.dtype)))
            except TypeError:
                pass
            for index in np.ndindex(temp.shape):
                try:
                    temp[index] = data[var][index]
                except AttributeError:
                    temp[index] = str(data[var][index])
            temp.setncattr_string('short_name', var)
        out_grp.setncattr_string('history', 'test group for netcdf_utils')
        output.setncattr_string('history', 'test file for netcdf_utils')
    return


def open_dataset(test_file, DatasetClass):
    try:
        return DatasetClass('http://localhost:8000/',
                            application=ServerSideFunctions(
                                            NetCDFHandler(test_file)))
    except OSError:
        return DatasetClass(test_file)


def generate_test_files(request, tmpdir):
    number = 3
    data_tmpdir = tmpdir.mkdir('data')
    for idx in range(number):
        file_name = data_tmpdir.join('test_{0}.nc'.format(idx))
        data = create_data()
        create_test_file(file_name, data, request.param, idx)
        yield str(file_name), data


@pytest.fixture(scope='function',
                params=[nc4_Dataset, h5_Dataset, pydap_Dataset])
def datasets_all(request):
    return request.param


@pytest.fixture(scope='function', params=['/'])
def test_files_root(request, tmpdir):
    return generate_test_files(request, tmpdir)


def test__sanitized_datatype(datasets_all, test_files_root):
    """
    Test that all datasets write a meaningful datatype
    to netCDF4.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets_all)) as dataset:
        test_file2, data2 = next(test_files_root)
        with nc4_Dataset(test_file2, 'a') as test_ds:
            for var in data.dtype.names:
                datatype = dc._sanitized_datatype(dataset, var)
                var_test = test_ds.createVariable(var + '_test', datatype)
                assert datatype == var_test.datatype
