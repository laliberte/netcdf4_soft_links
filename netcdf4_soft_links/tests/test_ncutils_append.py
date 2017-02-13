"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from contextlib import closing

from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import append as au
from netcdf4_soft_links.ncutils import replicate as ru

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


def test_append_full_netcdf_recursive(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not yet handled unlimited dimensions')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, datasets)) as dataset2:
            with closing(nc4_Dataset(test_file3, 'w')) as output:
                ru.replicate_full_netcdf_recursive(dataset2, output)
            with closing(nc4_Dataset(test_file3, 'a')) as output:
                record_dimensions = au.append_record(dataset, output)
                np.testing.assert_equal(record_dimensions,
                                        {'time': {'append_slice': [2, 0]}})
            with closing(nc4_Dataset(test_file3, 'a')) as output:
                output = au.append_and_copy_variable(dataset, output,
                                                     'temperature',
                                                     record_dimensions)
                output = au.append_and_copy_variable(dataset, output,
                                                     'lat_bnds',
                                                     record_dimensions)
            with closing(nc4_Dataset(test_file3, 'r')) as output:
                np.testing.assert_equal(
                        output.variables['temperature'][:],
                        np.concatenate([dataset.variables['temperature']
                                        [1, ...][np.newaxis, ...],
                                        dataset2.variables['temperature']
                                        [1, ...][np.newaxis, ...],
                                        dataset.variables['temperature']
                                        [0, ...][np.newaxis, ...]],
                                       axis=0))


@pytest.mark.skip(reason='Dask does work yet')
def test_append_full_netcdf_recursive_dask(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not yet handled unlimited dimensions')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, datasets)) as dataset2:
            with closing(nc4_Dataset(test_file3, 'w')) as output:
                ru.replicate_full_netcdf_recursive(dataset2, output,
                                                   allow_dask=True)
            with closing(nc4_Dataset(test_file3)) as output:
                np.testing.assert_equal(output.variables['temperature'][:],
                                        dataset2.variables['temperature'][:])
            with closing(nc4_Dataset(test_file3, 'a')) as output:
                record_dimensions = au.append_record(dataset, output)
                np.testing.assert_equal(record_dimensions,
                                        {'time': {'append_slice': [2, 0]}})
            with closing(nc4_Dataset(test_file3, 'a')) as output:
                output = au.append_and_copy_variable(dataset, output,
                                                     'temperature',
                                                     record_dimensions,
                                                     allow_dask=True)
                output = au.append_and_copy_variable(dataset, output,
                                                     'lat_bnds',
                                                     record_dimensions,
                                                     allow_dask=True)
            with closing(nc4_Dataset(test_file3, 'r')) as output:
                np.testing.assert_equal(
                        output.variables['temperature'][:].filled(),
                        np.concatenate([dataset.variables['temperature']
                                        [1, ...][np.newaxis, ...],
                                        dataset2.variables['temperature']
                                        [1, ...][np.newaxis, ...],
                                        dataset.variables['temperature']
                                        [0, ...][np.newaxis, ...]],
                                       axis=0))
