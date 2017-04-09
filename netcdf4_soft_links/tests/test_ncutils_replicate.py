"""
    Test the DAP handler, which forms the core of the client.
"""

from contextlib import closing
import netCDF4


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import replicate as ru
from netcdf4_soft_links.ncutils.compare_dataset import check_netcdf_equal

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


def test_replicate_full_netcdf_recursive(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_dask(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not works')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               allow_dask=True)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_groups(datasets,
                                                test_files_groups):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not works')
    test_file, data = next(test_files_groups)
    test_file2, data2 = next(test_files_groups)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_slice(datasets,
                                               test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not works')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            slices = {'lat': slice(0, 1, 1)}
            ru.replicate_full_netcdf_recursive(dataset, output, slices=slices)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output, slices=slices)


def test_replicate_full_netcdf_recursive_slice2(datasets,
                                                test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not works')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            def slices(x):
                return {'lat': slice(0, 1, 1)}
            ru.replicate_full_netcdf_recursive(dataset, output, slices=slices)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output, slices=slices(None))


def test_replicate_full_netcdf_recursive_scalar(datasets,
                                                test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not works')
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(netCDF4.Dataset(test_file, 'a')) as dataset:
        # Create scalar:
        temp = dataset.createVariable('scalar', 'f', ())
        temp[:] = 1e20

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_orphan(datasets,
                                                test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets != nc4_Dataset:
        pytest.xfail(reason='Only netCDF4 works')

    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(netCDF4.Dataset(test_file, 'a')) as dataset:
        # Create orphan dimension:
        dataset.createDimension('test', 10)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output)
            output.sync()
        with closing(netCDF4.Dataset(test_file2, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_noninc(datasets,
                                                test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='List indexing only works for netCDF4')

    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0]})
            output.sync()
    with closing(open_dataset(test_file2,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0]})

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_noninc_dask(datasets,
                                                     test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets == pydap_Dataset:
        pytest.xfail(reason='List indexing only works for netCDF4')

    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0]},
                                               allow_dask=True)
            output.sync()
    with closing(open_dataset(test_file2,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0]},
                                               allow_dask=True)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


def test_replicate_full_netcdf_recursive_noninc_mult(datasets,
                                                     test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    if datasets != nc4_Dataset:
        pytest.xfail(reason='List indexing only works for netCDF4')

    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0],
                                                       'lat': [1, 0]})
            output.sync()
    with closing(open_dataset(test_file2,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0],
                                                       'lat': [1, 0]})

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'r')) as output:
            assert check_netcdf_equal(dataset, output)


@pytest.mark.xfail(reason='List indexing does not work for dask')
def test_replicate_full_netcdf_recursive_noninc_mult_dask(datasets,
                                                          test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """

    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    test_file3, data3 = next(test_files_root)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file2, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0],
                                                       'lat': [1, 0]},
                                               allow_dask=True)
            output.sync()
    with closing(open_dataset(test_file2,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'w')) as output:
            ru.replicate_full_netcdf_recursive(dataset, output,
                                               slices={'time': [1, 0],
                                                       'lat': [1, 0]},
                                               allow_dask=True)

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        with closing(netCDF4.Dataset(test_file3, 'r')) as output:
            assert check_netcdf_equal(dataset, output)
