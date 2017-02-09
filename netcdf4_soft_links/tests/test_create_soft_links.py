"""
    Test the DAP handler, which forms the core of the client.
"""

from contextlib import closing
import numpy as np
import os
import shutil

from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.soft_links import create_soft_links
from netcdf4_soft_links.remote_netcdf.http_netcdf import checksum_for_file

from netcdf4_soft_links.netcdf4_pydap import Dataset as pydap_Dataset
from netCDF4 import Dataset as nc4_Dataset
from h5netcdf.legacyapi import Dataset as h5_Dataset

import pytest

from netcdf4_soft_links.data.testing_data import generate_test_files
from netcdf4_soft_links.data\
     .testing_data_fx import generate_test_files as generate_test_files_fx


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


@pytest.fixture(scope='function',
                params=[nc4_Dataset, h5_Dataset])
def outputs(request):
    return request.param


@pytest.fixture(scope='function', params=['/', 'g1/g2/g3'])
def test_files_groups(request, tmpdir):
    return generate_test_files(request, tmpdir)


@pytest.fixture(scope='function', params=['/'])
def test_files_root(request, tmpdir):
    return generate_test_files(request, tmpdir)


@pytest.fixture(scope='function', params=['/'])
def test_files_fx(request, tmpdir):
    return generate_test_files_fx(request, tmpdir)


def test_record_paths(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(paths_list,
                                                                   'day')
        data_collection.record_paths(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])


def test_record_metadata(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(paths_list,
                                                                   'day')
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])


def test_record_metadata_check_dimensions(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(
                                            paths_list, 'day',
                                            check_dimensions=True)
        data_collection.record_meta_data(output, ['temperature'])

    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])


def test_record_metadata_fx(datasets, outputs, test_files_fx):
    test_file, data = next(test_files_fx)
    test_file2, data2 = next(test_files_fx)
    test_file3, data3 = next(test_files_fx)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(paths_list,
                                                                   'fx')
        data_collection.record_meta_data(output, 'temperature')
        data_collection.record_meta_data(output, ['number'])
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])


def test_record_metadata_duplicate(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    alt_dir = os.path.dirname(test_file) + '/alt'
    os.mkdir(alt_dir)
    alt_test_file = alt_dir + '/' + os.path.basename(test_file)
    shutil.copy(test_file, alt_test_file)
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                      [alt_test_file, checksum_type,
                       checksum_for_file(checksum_type, alt_test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(paths_list,
                                                                   'day')
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        try:
            np.testing.assert_equal(paths, [test_file2, alt_test_file,
                                            test_file])
            np.testing.assert_equal(paths_id, [hash(test_file2),
                                               hash(alt_test_file),
                                               hash(test_file)])
        except AssertionError:
            np.testing.assert_equal(paths, [test_file2, test_file,
                                            alt_test_file])
            np.testing.assert_equal(paths_id, [hash(test_file2),
                                               hash(test_file),
                                               hash(alt_test_file)])


def test_record_metadata_diff_times(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    with closing(nc4_Dataset(test_file, 'a')) as dataset:
        dataset.variables['time'].setncattr_string('units',
                                                   'days since 1979-12-31')
        dataset.variables['time'].setncattr_string('calendar',
                                                   'proleptic_gregorian')
        dataset.variables['time'][:] = dataset.variables['time'][:] + 1.0

    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(paths_list,
                                                                   'day')
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])


def test_record_metadata_time_restriction(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not support groups')
    checksum_type = 'SHA256'
    paths_list = [{'path': '|'.join(
                        [test_file, checksum_type,
                         checksum_for_file(checksum_type, test_file), '1a']),
                   'file_type': 'local_file', 'version': 'v1'},
                  {'path': '|'.join(
                        [test_file2, checksum_type,
                         checksum_for_file(checksum_type, test_file2), '2b']),
                   'file_type': 'local_file', 'version': 'v2'}]
    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(
                                                paths_list, 'day',
                                                years=[1980],
                                                months=[1])
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])

    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(
                                                paths_list, 'day',
                                                years=[1980])
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])

    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(
                                                paths_list, 'day',
                                                years=[0])
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])

    with closing(open_dataset(test_file3, outputs, mode='w')) as output:
        data_collection = create_soft_links.create_netCDF_pointers(
                                                paths_list, 'day',
                                                months=[1])
        data_collection.record_meta_data(output, 'temperature')
    with closing(open_dataset(test_file3, datasets)) as dataset:
        paths = dataset.groups['soft_links'].variables['path'][:]
        np.testing.assert_equal(paths, [test_file2, test_file])
        paths_id = dataset.groups['soft_links'].variables['path_id'][:]
        np.testing.assert_equal(paths_id, [hash(test_file2), hash(test_file)])
