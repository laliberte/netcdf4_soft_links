"""
Test some indices utils
"""
import numpy as np
from netcdf4_soft_links.ncutils import indices as iu


def test_largest_hyperslab():
    largest = iu.largest_hyperslab({'time': [slice(0, 100, 10)],
                                    'lat': [slice(0, 5, 1)]})
    assert largest == 50


def test_take_safely():
    out = iu.take_safely(np.empty((2, 0, 2)), [0, 1], axis=1)
    np.testing.assert_equal(out.filled(),
                            np.ma.masked_all((2, 2, 2)).filled())


def test_slice_a_slice():
    assert iu.slice_a_slice(slice(0, 10, 2), slice(0, 5, 2)) == slice(0, 9, 4)
    np.testing.assert_equal(iu.slice_a_slice(slice(0, 10, 2), [0, 1]),
                            [0, 2])
    np.testing.assert_equal(iu.slice_a_slice([0, 2, 4, 6, 8], [0, 1]),
                            [0, 2])


def test_get_indices_from_dim():
    np.testing.assert_equal(iu.get_indices_from_dim([1], [0]), [0])
