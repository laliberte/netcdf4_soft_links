# This Python file uses the following encoding: utf-8


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    import os
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

from setuptools import setup, find_packages

package_name='netcdf4_soft_links'
setup(
       name = package_name,
       version = "0.7.7.1",
       packages = find_packages(),
#
#        # metadata for upload to PyPI
        author = "F. B. Laliberte P. J. Kushner",
        author_email = "frederic.laliberte@utoronto.ca",
        description = "Tools to create soft links to netcdf4 files.",
        license = "BSD",
        keywords = "atmosphere climate",
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Science/Research",
            "Natural Language :: English",
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Fortran",
            "Topic :: Scientific/Engineering :: Mathematics"
        ],
        long_description=read('README.rst'),
        install_requires = ['numpy',
                            'h5py',
                            'h5netcdf>=0.3',
                            'netCDF4',
                            'netcdf4_pydap>=0.1.3',
                            'requests>=1.1.0',
                            'requests_cache'
                            ],
        zip_safe=False,
        # other arguments here...
        entry_points = {
                  'console_scripts': [
                           'nc4sl='+package_name+'.core:main'
                                     ],
                       }
    )
