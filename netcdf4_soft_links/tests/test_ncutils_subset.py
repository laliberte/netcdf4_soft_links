import numpy as np

from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import subset

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


def test_region_mask_positive():
    lonlatbox = [100., 250., 30.0, 60.0]
    for lon, lat, mask in [(200.0, 50.0, True),
                           (260.0, 50.0, False),
                           (-5.0, 50.0, False),
                           (-120.0, 50.0, True)]:
        assert(np.all(subset.get_region_mask(
                                            np.array([[[lat]]]),
                                            np.array([[[lon]]]),
                                            lonlatbox) == np.array([[mask]])))


def test_region_mask_negative():
    lonlatbox = [250.0, 100.0, 30.0, 60.0]
    for lon, lat, mask in [(200.0, 50.0, False),
                           (260.0, 50.0, True),
                           (-5.0, 50.0, True),
                           (-120.0, 50.0, False)]:
        assert(np.all(subset.get_region_mask(
                                            np.array([[[lat]]]),
                                            np.array([[[lon]]]),
                                            lonlatbox) == np.array([[mask]])))


@pytest.mark.skip(reason="Does not work at the moment")
def test_subset(test_files_groups):
    test_file1, data1 = next(test_files_groups)
    test_file2, data2 = next(test_files_groups)

    subset.subset(test_file1, test_file2, lonlatbox=[100.0, 210.0,
                                                     -90.0, 90.0])
    with nc4_Dataset(test_file2, 'r') as dataset:
        def check_subset(grp_name):
            np.testing.assert_equal(dataset[grp_name + 'lat'][:],
                                    [0.0, 90.0])
            np.testing.assert_equal(dataset[grp_name + 'lon'][:],
                                    [180.0])
            return True
        if 'g1' in dataset.groups:
            assert check_subset('g1/g2/g3/')
        else:
            assert check_subset('/')
