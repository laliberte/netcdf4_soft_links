netcdf4_soft_links
==================
|Build Status|

.. |Build Status| image:: https://travis-ci.org/laliberte/cdb_query.svg
   :target: https://travis-ci.org/laliberte/cdb_query

Python code to create soft links to netcdf4 files.

Intended to process data from the CMIP5 and CORDEX archives distributed 
by the Earth System Grid Federation.

This package was developed by F. B. Laliberte and P. J. Kushner as part of the "ExArch: Climate analytics
on distributed exascale data archives" G8 Research Initiative grant.

Note that at the moment testing of ``netcdf4_soft_links`` is done entirely through ``cdb_query``.
The build status shown above actually reflects the build status of ``cdb_query``.

Frederic Laliberte, Paul J. Kushner
Univerty of Toronto, 2016

The Natural Sciences and Engineering Research Council of Canada (NSERC/CRSNG) funded 
FBL and PJK during this project.

Version History
---------------

0.7.x:  Minor bug fixes for handling years prior to 1900.
Performance improvements in reading soft links.

0.7:    Improved pydap - requests interactions. Cleaner temp file handling.

0.6:    Major overhaul in certificate handling management. In the future, certficates
might become unnecessary.

0.5.x:  Minor bug fixes. Bug fix with fixed time variables. Fixed download_files.
Added subset function. Fixed import errors. Minor changes to read_soft_links API.

0.5 :   Version in support of upcoming cdb_query 2.0

0.3 :   Several updates. Bug fixes and streamlining. Inclusion of pydap.

0.2 :   First fully functional version.

0.1 :   Initial commit.
