"""
    Test the DAP handler, which forms the core of the client.
"""
from contextlib import closing

from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import dataset_compat as dc

from netcdf4_soft_links.netcdf4_pydap import Dataset as pydap_Dataset
from netCDF4 import Dataset as nc4_Dataset
from h5netcdf.legacyapi import Dataset as h5_Dataset

import pytest
from netcdf4_soft_links.data.testing_data import generate_test_files


def open_dataset(test_file, DatasetClass, mode='r'):
    if DatasetClass == pydap_Dataset:
        return DatasetClass('http://localhost:8000/',
                            application=ServerSideFunctions(
                                            NetCDFHandler(test_file)))
    else:
        return DatasetClass(test_file, mode=mode)


@pytest.fixture(scope='function',
                params=[nc4_Dataset, h5_Dataset, pydap_Dataset])
def datasets(request):
    return request.param


@pytest.fixture(scope='function', params=['/'])
def test_files_root(request, tmpdir):
    return generate_test_files(request, tmpdir)


def test__sanitized_datatype(datasets, test_files_root):
    """
    Test that all datasets write a meaningful datatype
    to netCDF4.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        test_file2, data2 = next(test_files_root)
        with nc4_Dataset(test_file2, 'a') as test_ds:
            for var in data.dtype.names:
                datatype = dc._sanitized_datatype(dataset, var)
                test_ds.createVariable(var + '_test', datatype)
                new_datatype = dc._sanitized_datatype(test_ds, var + '_test')
                assert datatype == new_datatype


def test__isunlimited(datasets, test_files_root):
    """
    Test that all datasets write a meaningful datatype
    to netCDF4.
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support unlimited dimension')

    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        for dim in dataset.dimensions:
            if dim == 'time':
                assert dc._isunlimited(dataset, dim)
            else:
                assert not dc._isunlimited(dataset, dim)

            with pytest.raises(KeyError):
                dc._isunlimited(dataset, 'missing_dim')
                pass


def test__dim_len(datasets, test_files_root):
    """
    Test that all datasets write a meaningful datatype
    to netCDF4.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        assert dc._dim_len(dataset, 'time') == 2


def test__shape(datasets, test_files_root):
    """
    Test that all datasets write a meaningful datatype
    to netCDF4.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        assert dc._shape(dataset, 'time_bnds') == (2, 2)
        assert dc._shape(dataset, 'temperature') == (2, 2, 2, 2)
