"""
    Test the DAP handler, which forms the core of the client.
"""

import numpy as np
from contextlib import closing
import datetime


from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_soft_links.netcdf4_pydap.netcdf4_pydap\
     .netcdf4_pydap.pydap_fork\
     .pydap.src.pydap.wsgi.ssf import ServerSideFunctions

from netcdf4_soft_links.ncutils import time as tu

from netcdf4_soft_links.netcdf4_pydap import Dataset as pydap_Dataset
from netCDF4 import Dataset as nc4_Dataset
from netCDF4 import num2date
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


def test_get_year_axis(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        # Default:
        year_axis, month_axis = tu.get_year_axis(dataset, default=True)
        np.testing.assert_array_equal(year_axis, [])
        np.testing.assert_array_equal(month_axis, [])
        # Relative time:
        year_axis, month_axis = tu.get_year_axis(dataset)
        np.testing.assert_array_equal(year_axis, [1980, 1980])
        np.testing.assert_array_equal(month_axis, [1, 1])
        # Absolute time:
        year_axis, month_axis = tu.get_year_axis(dataset, time_var='time_abs')
        np.testing.assert_array_equal(year_axis, [1980, 1980])
        np.testing.assert_array_equal(month_axis, [1, 1])


def test_get_year_axis_edge(datasets, test_files_root):
    """
    Test that the special cases:
    1) days since year 0 works.
    2) no calendar specified
    Use only netCDF4 datasets.
    """
    test_file, data = next(test_files_root)
    with nc4_Dataset(test_file, 'a') as dataset:
        (dataset.variables['time']
         .setncattr_string('units', 'days since 0-01-01 00:00:00'))
        dataset.variables['time'].setncattr_string('calendar', '365_day')
        dataset.variables['time_abs'].delncattr('calendar')

    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        # Relative time:
        year_axis, month_axis = tu.get_year_axis(dataset, time_var='time')
        np.testing.assert_array_equal(year_axis, [0, 0])
        np.testing.assert_array_equal(month_axis, [1, 1])
        year_axis, month_axis = tu.get_year_axis(dataset, time_var='time_abs')
        np.testing.assert_array_equal(year_axis, [1980, 1980])
        np.testing.assert_array_equal(month_axis, [1, 1])


def test_get_date_axis(datasets, test_files_root):
    """
    Test that the special case of days since year 0 works.
    Use only netCDF4 datasets.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        # Default:
        np.testing.assert_array_equal([], tu.get_date_axis(dataset,
                                                           default=True))
        # Relative time:
        date_axis = tu.get_date_axis(dataset, 'time')
        np.testing.assert_array_equal(date_axis,
                                      [datetime.datetime(1980, 1, 1),
                                       datetime.datetime(1980, 1, 2)])


def test_get_date_axis_from_calendar_and_units():
    np.testing.assert_array_equal(
                tu.get_date_axis_from_units_and_calendar(default=True),
                np.array([]))


def test_get_date_axis_edge(datasets, test_files_root):
    """
    Test that the special cases:
    1) days since year 0 works.
    2) no calendar specified
    Use only netCDF4 datasets.
    """
    test_file, data = next(test_files_root)
    edge_units = 'days since 0-01-01 00:00:00'
    working_units = 'days since 01-01-01 00:00:00'
    with nc4_Dataset(test_file, 'a') as dataset:
        (dataset.variables['time']
         .setncattr_string('units', edge_units))
        dataset.variables['time'].setncattr_string('calendar', '365_day')
        dataset.variables['time_abs'].delncattr('calendar')
        dataset.sync()

    with closing(open_dataset(test_file, datasets)) as dataset:
        # Relative time:
        date_axis = tu.get_date_axis(dataset, time_var='time')
        np.testing.assert_array_equal(date_axis,
                                      [num2date(-365.0, units=working_units,
                                                calendar='365_day'),
                                       num2date(-364.0, units=working_units,
                                                calendar='365_day')])
        date_axis = tu.get_date_axis(dataset, time_var='time_abs')
        np.testing.assert_array_equal(date_axis,
                                      [datetime.datetime(1980, 1, 1),
                                       datetime.datetime(1980, 1, 2)])


def test_get_time(datasets, test_files_root):
    """
    Test get_time
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        # Default:
        np.testing.assert_array_equal([], tu.get_time(dataset,
                                                      default=True))
        date_axis = tu.get_time(dataset)
        np.testing.assert_array_equal(date_axis,
                                      [datetime.datetime(1980, 1, 1),
                                       datetime.datetime(1980, 1, 2)])
        date_axis = tu.get_time(dataset, time_var='time_abs')
        np.testing.assert_array_equal(date_axis,
                                      [datetime.datetime(1980, 1, 1),
                                       datetime.datetime(1980, 1, 2)])


def test_get_time_axis_relative():
    time_axis = tu.get_time_axis_relative([datetime.datetime(1980, 1, 1)],
                                          'days since 1980-01-01 00:00:00',
                                          None)
    assert time_axis == np.array([0])
    time_axis = tu.get_time_axis_relative([datetime.datetime(1980, 1, 1)],
                                          'days since 1980-01-01 00:00:00',
                                          '360_day')
    assert time_axis == np.array([0])
    np.testing.assert_equal(tu.get_time_axis_relative(default=True),
                            np.array([]))


def test_get_date_axis_relative():
    date_axis = tu.get_date_axis_relative([0],
                                          'days since 1980-01-01 00:00:00',
                                          None)
    assert date_axis == np.array([datetime.datetime(1980, 1, 1)])
    date_axis = tu.get_date_axis_relative([365*1980],
                                          'days since 0-01-01 00:00:00',
                                          '365_day')
    assert (np.array([datetime.datetime(date.year, date.month, date.day,
                                        date.hour, date.minute)
                      for date in date_axis]) ==
            np.array([datetime.datetime(1980, 1, 1)]))
    np.testing.assert_equal(tu.get_date_axis_relative(default=True),
                            np.array([]))


def test_get_date_axis_absolute():
    date_axis = tu.get_date_axis_absolute([19800101.0])
    assert date_axis == np.array([datetime.datetime(1980, 1, 1)])
    np.testing.assert_equal(tu.get_date_axis_absolute(default=True),
                            np.array([]))


def test_netcdf_calendar(datasets, test_files_root):
    """
    Test recovery of calendar
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert tu.netcdf_calendar(dataset) == 'standard'
        assert tu.netcdf_calendar(default=True) == 'standard'


def test_find_time_dim(datasets, test_files_root):
    """
    Test infer time dimension
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert tu.find_time_dim(dataset) == 'time'
        assert tu.find_time_dim(default=True) == 'time'
        assert tu.find_time_dim(default=True, time_var='time') == 'time'


def test_find_time_var(datasets, test_files_root):
    """
    Test infer time variable
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert tu.find_time_var(dataset) == 'time'
        assert tu.find_time_var(default=True) == 'time'
        assert tu.find_time_var(default=True, time_var='time') == 'time'


def test_ensure_compatible_time_units_overlapping(datasets, test_files_root):
    """
    Test that time units are compatible with overlapping dimensions
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(nc4_Dataset(test_file2, mode='a')) as output:
            np.testing.assert_allclose(
                tu.ensure_compatible_time_units(dataset, output, 'time',
                                                default=True),
                dataset.variables['time'][:])
            np.testing.assert_allclose(
                tu.ensure_compatible_time_units(dataset, output, 'time'),
                [2, 0])


def test_ensure_compatible_time_units_non_overlapping(datasets,
                                                      test_files_root):
    """
    Test that time units are compatible with non-overlapping dimensions
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(nc4_Dataset(test_file2, mode='a')) as output:
            output.variables['time'][:] = output.variables['time'][:] + 1.0
            output.sync()
            assert (tu.ensure_compatible_time_units(dataset, output, 'time') ==
                    slice(2, 4, 1))


def test_ensure_compatible_time_units_no_units(datasets, test_files_root):
    """
    Test that time units are compatible with no units
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(nc4_Dataset(test_file2, mode='a')) as output:
            output.variables['time'].delncattr('units')
            output.sync()
            np.testing.assert_allclose(
                tu.ensure_compatible_time_units(dataset, output, 'time'),
                [2, 0])


def test_create_time_axis(datasets, test_files_root):
    """
    Test that time axis creation works.
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(nc4_Dataset(test_file2, mode='w')) as output:
            output = tu.create_time_axis(dataset, output,
                                         dataset.variables['time'][:])
            assert 'time' in output.variables
            assert 'units' in output.variables['time'].ncattrs()
            assert (output.variables['time'].getncattr('units') ==
                    'days since 1980-01-01')
            assert 'calendar' in output.variables['time'].ncattrs()
            assert (output.variables['time'].getncattr('calendar') ==
                    'standard')
            np.testing.assert_allclose(output.variables['time'][:],
                                       [0, 1])


def test_create_time_axis_no_dataset(datasets, test_files_root):
    """
    Test that time axis creation works.
    """
    test_file, data = next(test_files_root)
    test_file2, data2 = next(test_files_root)
    with closing(open_dataset(test_file, datasets)) as dataset:
        with closing(nc4_Dataset(test_file2, mode='w')) as output:
            output = tu.create_time_axis(None, output,
                                         dataset.variables['time'][:])
            assert 'time' in output.variables
            assert 'units' in output.variables['time'].ncattrs()
            assert (output.variables['time'].getncattr('units') ==
                    'days since 0.0')
            assert 'calendar' in output.variables['time'].ncattrs()
            assert (output.variables['time'].getncattr('calendar') ==
                    'standard')
            np.testing.assert_allclose(output.variables['time'][:],
                                       [0, 1])

            outdef = tu.create_time_axis(None, output,
                                         dataset.variables['time'][:],
                                         default=True)
            assert output == outdef


def test_create_date_axis(test_files_root):
    """
    Test that time axis creation works.
    """
    test_file, data = next(test_files_root)
    with closing(nc4_Dataset(test_file, mode='w')) as output:
        output = tu.create_time_axis_date(output,
                                          [datetime.datetime(1980, 1, 1)],
                                          'days since 1980-01-01',
                                          'standard')
        assert 'time' in output.variables
        assert 'units' in output.variables['time'].ncattrs()
        assert (output.variables['time'].getncattr('units') ==
                'days since 1980-01-01')
        assert 'calendar' in output.variables['time'].ncattrs()
        assert (output.variables['time'].getncattr('calendar') ==
                'standard')
        np.testing.assert_allclose(output.variables['time'][:],
                                   [0])


def test_variables_list_with_time_dim(datasets, test_files_root):
    """
    Test that time axis creation works.
    """
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert (sorted(tu.variables_list_with_time_dim(dataset, 'time')) ==
                sorted(['time_abs', 'time', 'flag', 'temperature', 'number',
                        'time_bnds']))
        assert tu.variables_list_with_time_dim(default=True) == []


def test_netcdf_time_units(datasets, test_files_root):
    test_file, data = next(test_files_root)
    with closing(open_dataset(test_file,
                              datasets)) as dataset:
        assert tu.netcdf_time_units(dataset, 'time') == 'days since 1980-01-01'
        assert tu.netcdf_time_units(default=True) is None


def test_create_date_axis_from_time_axis():
    attributes = {'units': 'days since 1980-01-01',
                  'calendar': 'standard'}
    time_axis = [0, 1]
    date_axis = tu.create_date_axis_from_time_axis(time_axis, attributes)
    np.testing.assert_equal(date_axis, [datetime.datetime(1980, 1, 1),
                                        datetime.datetime(1980, 1, 2)])
    attributes = {'units': 'day as %Y%m%d.%f',
                  'calendar': 'standard'}
    time_axis = [19800101.0, 19800102.0]
    date_axis = tu.create_date_axis_from_time_axis(time_axis, attributes)
    np.testing.assert_equal(date_axis, [datetime.datetime(1980, 1, 1),
                                        datetime.datetime(1980, 1, 2)])
    np.testing.assert_equal(tu.create_date_axis_from_time_axis(default=True),
                            np.array([]))
