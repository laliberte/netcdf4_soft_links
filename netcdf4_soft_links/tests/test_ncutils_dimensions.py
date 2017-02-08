"""
p
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from contextlib import closing
from collections import OrderedDict


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import dimensions as du

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


def test_retrieve_dimension(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        time_axis, attrs = du.retrieve_dimension(dataset, 'time')
        np.testing.assert_equal(time_axis, range(2))
        assert attrs == {'calendar': 'standard',
                         'units': 'days since 1980-01-01',
                         'bounds': 'time_bnds'}


def test_retrieve_dimension_orphan(datasets, test_files_root):
    if datasets != nc4_Dataset:
        pytest.xfail(reason='Only works for netCDF4')
    test_file, data = next(test_files_root)
    with closing(nc4_Dataset(test_file, 'a')) as dataset:
        dataset.createDimension('test', 10)
    with closing(open_dataset(test_file, datasets)) as dataset:
        test_axis, attrs = du.retrieve_dimension(dataset, 'test')
        np.testing.assert_equal(test_axis, range(10))
        assert attrs == dict()


def test_dimension_compatibility(datasets, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, datasets)) as output:
            assert du.dimension_compatibility(dataset, output, 'time')


def test_check_dimensions_compatibility(datasets, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, datasets)) as output:
            assert du.check_dimensions_compatibility(dataset, output,
                                                     'temperature')


def test_get_dataset_with_dimension(datasets, test_files_groups):
    test_file, data = next(test_files_groups)
    if datasets == pydap_Dataset:
        pytest.skip('PYDAP does not have groups')
    with closing(open_dataset(test_file, datasets)) as dataset:
        if 'g1' in dataset.groups:
            assert (du.get_dataset_with_dimension(dataset['g1/g2'], 'time') ==
                    dataset['g1'])
        else:
            assert (du.get_dataset_with_dimension(dataset, 'time') ==
                    dataset)


def test__is_dimension_present(datasets, test_files_groups):
    test_file, data = next(test_files_groups)
    if datasets == pydap_Dataset:
        pytest.skip('PYDAP does not have groups')
    with closing(open_dataset(test_file, datasets)) as dataset:
        if 'g1' in dataset.groups:
            assert not du._is_dimension_present(dataset['g1/g2'], 'time')
        else:
            assert du._is_dimension_present(dataset, 'time')


def test_find_dimension_type(datasets, test_files_root):
    test_file, data = next(test_files_root)
    if datasets == pydap_Dataset:
        pytest.skip('PYDAP does not have groups')
    with closing(open_dataset(test_file, datasets)) as dataset:
        print(du.find_dimension_type(dataset))
        assert (dict(du.find_dimension_type(dataset)) ==
                dict([('bnds', 2), ('lat', 2), ('plev', 2),
                      ('lon', 2)]))
