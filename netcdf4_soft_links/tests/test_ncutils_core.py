"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from contextlib import closing
import six


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import core as cu

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


@pytest.fixture(scope='function', params=['/', 'g1/g2/g3'])
def test_files_groups(request, tmpdir):
    return generate_test_files(request, tmpdir)


@pytest.fixture(scope='function', params=['/'])
def test_files_root(request, tmpdir):
    return generate_test_files(request, tmpdir)


def test_check_if_opens(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        # Default:
        assert not cu.check_if_opens(dataset, default=True)
        assert cu.check_if_opens(dataset)


def test_getncattr(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert cu.getncattr(dataset, 'history') == 'test file for netcdf_utils'
        np.testing.assert_allclose(cu.getncattr(dataset
                                                .variables['temperature'],
                                                'chunksizes'), (1, 2, 2, 2))


def test_setncattr(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(nc4_Dataset(test_file, mode='a')) as dataset:
        cu.setncattr(dataset, 'history', 'test modify history')
        cu.setncattr(dataset, 'vector', [1, 3])
    with closing(open_dataset(test_file, datasets)) as dataset:
        assert cu.getncattr(dataset, 'history') == 'test modify history'
        np.testing.assert_allclose(cu.getncattr(dataset, 'vector'), [1, 3])


def test_maybe_conv_bytes_to_str():
    source = np.array([[b'test', b'a'],
                       [b'b', b'c']], dtype='O')
    if six.PY3:
        assert source.item(0) != 'test'
    assert cu.maybe_conv_bytes_to_str_array(source).item(0) == 'test'


def test_find_time_name_from_list():
    assert cu.find_time_name_from_list(['TIME', 'time_alt'], 'time') == 'TIME'
    assert cu.find_time_name_from_list(['TIME', 'time_alt'], 'date') is None
