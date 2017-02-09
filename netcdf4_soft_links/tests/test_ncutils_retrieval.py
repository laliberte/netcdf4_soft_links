"""
    Test the DAP handler, which forms the core of the client.
"""

from contextlib import closing
import numpy as np


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import retrieval as ru
from netcdf4_soft_links.ncutils.indices import prepare_indices
from netcdf4_soft_links.ncutils.compare_dataset import check_var_equal

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


def test_retrieve_dimension_list(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        dimensions = ru.retrieve_dimension_list(dataset, 'temperature')
        assert sorted(dimensions) == sorted(['time', 'plev', 'lat', 'lon'])


def test_retrieve_dimensions_no_time(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        dimensions, attrs = ru.retrieve_dimensions_no_time(dataset,
                                                           'temperature')
        np.testing.assert_equal(dimensions,
                                {'lon': np.array([0., 360.]),
                                 'lat': np.array([0., 90.]),
                                 'plev': np.array([100000., 10000.])})
        assert attrs == {'lon': {'bounds': 'lon_bnds'},
                         'lat': {'bounds': 'lat_bnds'},
                         'plev': {'bounds': 'plev_bnds'}}


def test_retrieve_variables(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, outputs, mode='w')) as output:
            output = ru.retrieve_variables(dataset, output)
            for var in dataset.variables:
                assert check_var_equal(dataset, output, var)


def test_retrieve_variables_no_time(datasets, outputs, test_files_root):
    test_file, data = next(test_files_root)
    test_file2, data = next(test_files_root)
    if outputs == h5_Dataset:
        pytest.xfail(reason='h5netcdf does not support dimensions creation')
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(open_dataset(test_file2, outputs, mode='w')) as output:
            output = ru.retrieve_variables_no_time(dataset, output, 'time')
            for var in dataset.variables:
                if 'time' not in dataset.variables[var].dimensions:
                    assert check_var_equal(dataset, output, var)


def test_retrieve_container1(datasets, test_files_root):
    test_file, data = next(test_files_root)
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not work by should')
    with closing(open_dataset(test_file, datasets)) as dataset:
        var = 'temperature'
        dimensions = dict()
        unsort_dimensions = dict()
        for dim in dataset.variables[var].dimensions:
            if dim != 'time':
                dimensions[dim] = dataset.variables[dim][:][::-1]
        (dimensions['time'],
         unsort_dimensions['time']) = prepare_indices(np.array([0, 1]))

        out = ru.retrieve_container(dataset, var, dimensions,
                                    unsort_dimensions, [], 450.0)
        np.testing.assert_equal(data[var][:, ::-1, ::-1, ::-1], out)


def test_retrieve_container2(datasets, test_files_root):
    test_file, data = next(test_files_root)
    if datasets == pydap_Dataset:
        pytest.xfail(reason='PYDAP does not work by should')
    with closing(open_dataset(test_file, datasets)) as dataset:
        var = 'temperature'
        dimensions = dict()
        unsort_dimensions = dict()
        for dim in dataset.variables[var].dimensions:
            if dim != 'time':
                dimensions[dim] = [dataset.variables[dim][:][1] + 1e-8]
        (dimensions['time'],
         unsort_dimensions['time']) = prepare_indices(np.array([0, 1]))

        out = ru.retrieve_container(dataset, var, dimensions,
                                    unsort_dimensions, [], 1e-10)
        np.testing.assert_equal(data[var][:, 1, 1, 1], np.squeeze(out))
