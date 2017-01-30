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

from netcdf4_soft_links import netcdf_utils

from netcdf4_soft_links.netcdf4_pydap import Dataset as pydap_Dataset
from netCDF4 import Dataset as nc4_Dataset
from h5netcdf.legacyapi import Dataset as h5_Dataset

import pytest


@pytest.fixture(scope='module')
def data():
    data = [(10, 15.2, 'Diamond_St'),
            (11, 13.1, 'Blacktail_Loop'),
            (12, 13.3, 'Platinum_St'),
            (13, 12.1, 'Kodiak_Trail')]
    return data


@pytest.fixture(scope='module')
def test_file(tmpdir_factory, data):

    # Create tempfile:
    test_file = str(tmpdir_factory.mktemp('data').join('test.nc'))
    with nc4_Dataset(test_file, 'w') as output:
        output.createDimension('index', None)
        temp = output.createVariable('index', np.dtype('int32'), ('index',))
        split_data = zip(*data)
        temp[:] = next(split_data)
        temp = output.createVariable('temperature', np.dtype('float32'), ('index',))
        temp[:] = next(split_data)
        temp = output.createVariable('station', np.dtype(str), ('index',))
        temp.setncattr('long_name', 'Station Name')
        for item_id, item in enumerate(next(split_data)):
            temp[item_id] = item
        output.createDimension('tag', 1)
        temp = output.createVariable('tag', '<i4', ('tag',))
        output.setncattr('history', 'test file for netCDF4 api')
    return test_file


@pytest.fixture(params=[nc4_Dataset, h5_Dataset, pydap_Dataset])
def dataset(request, test_file):
    try:
        return request.param('http://localhost:8000/',
                             application=ServerSideFunctions(
                                                NetCDFHandler(test_file)))
    except OSError:
        return request.param(test_file)


def test__sanitized_datatype(dataset):
    with closing(dataset):
        for var_name, type_name in [('temperature', 'float32'),
                                    ('station', str),
                                    ('index', 'int32')]:
            datatype = netcdf_utils._sanitized_datatype(dataset, var_name)
            assert datatype == np.dtype(type_name)
