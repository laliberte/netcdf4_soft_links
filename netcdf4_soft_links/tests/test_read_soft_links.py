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

from netcdf4_soft_links.soft_links import create_soft_links
from netcdf4_soft_links.soft_links import read_soft_links
from netcdf4_soft_links.remote_netcdf.http_netcdf import checksum_for_file
from netcdf4_soft_links.ncutils.compare_dataset import check_netcdf_equal

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


def test_record_paths_replicate(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    test_file4, data4 = next(test_files_root)
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
    with closing(open_dataset(test_file4, outputs, mode='w')) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            sl_data = read_soft_links.read_netCDF_pointers(dataset)
            sl_data.replicate(output)
    with closing(open_dataset(test_file4, datasets)) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            assert check_netcdf_equal(dataset, output)


def test_record_paths_append(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    test_file4, data4 = next(test_files_root)
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
    with closing(open_dataset(test_file4, outputs, mode='a')) as output:
        with closing(open_dataset(test_file3, outputs)) as dataset:
            sl_data = read_soft_links.read_netCDF_pointers(dataset)
            sl_data.append(output)
    with closing(open_dataset(test_file4, datasets)) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            assert check_netcdf_equal(dataset, output)


def test_record_metadata_replicate(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    test_file4, data4 = next(test_files_root)
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
                                        paths_list, 'day', data_node_list=[],
                                        record_other_vars=False)
        data_collection.record_meta_data(output, 'temperature')
        print(output)
        data_collection.record_meta_data(output, 'number')
        # Do not record 'flag' because empty string variables
        # are not cleanly handled by netCDF4-python
    with closing(open_dataset(test_file4, outputs, mode='w')) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            sl_data = read_soft_links.read_netCDF_pointers(dataset)
            sl_data.replicate(output)
    with closing(open_dataset(test_file4, datasets)) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            assert check_netcdf_equal(dataset, output)


def test_record_metadata_append(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)
    test_file4, data4 = next(test_files_root)
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
    for file_name in [test_file3, test_file4]:
        with closing(open_dataset(file_name, outputs, mode='w')) as output:
            data_collection = create_soft_links.create_netCDF_pointers(
                                          paths_list, 'day', data_node_list=[],
                                          record_other_vars=False)
            data_collection.record_meta_data(output, 'temperature')
            print(output)
            data_collection.record_meta_data(output, 'number')
            # Do not record 'flag' because empty string variables
            # are not cleanly handled by netCDF4-python
    with closing(open_dataset(test_file3, outputs, mode='a')) as output:
        with closing(open_dataset(test_file4, datasets)) as dataset:
            sl_data = read_soft_links.read_netCDF_pointers(dataset)
            sl_data.append(output)
    with closing(open_dataset(test_file4, datasets)) as output:
        with closing(open_dataset(test_file3, datasets)) as dataset:
            assert check_netcdf_equal(dataset, output)
