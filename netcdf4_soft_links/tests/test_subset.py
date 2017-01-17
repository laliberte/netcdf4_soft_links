import netcdf4_soft_links.subset.subset_utils as subset_utils
import numpy as np


def test_region_mask_positive():
    lonlatbox = [100., 250., 30.0, 60.0]
    for lon, lat, mask in [(200.0, 50.0, True),
                           (260.0, 50.0, False),
                           (-5.0, 50.0, False),
                           (-120.0, 50.0, True)]:
        assert(np.all(subset_utils.get_region_mask(
                                            np.array([[[lat]]]),
                                            np.array([[[lon]]]),
                                            lonlatbox) == np.array([[mask]])))


def test_region_mask_negative():
    lonlatbox = [250.0, 100.0, 30.0, 60.0]
    for lon, lat, mask in [(200.0, 50.0, False),
                           (260.0, 50.0, True),
                           (-5.0, 50.0, True),
                           (-120.0, 50.0, False)]:
        assert(np.all(subset_utils.get_region_mask(
                                            np.array([[[lat]]]),
                                            np.array([[[lon]]]),
                                            lonlatbox) == np.array([[mask]])))
