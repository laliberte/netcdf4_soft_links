from netcdf4_soft_links import parsers

import pytest


def test_parsers_no_args(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl'])
    out, err = capsys.readouterr()
    assert ("{subset,validate,"
            "download_files,download_opendap,load}") in err


def test_parsers_help(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', '-h'])
    out, err = capsys.readouterr()
    help_string = """
This script aggregates soft links to OPENDAP or local files.    

positional arguments:
  {subset,validate,download_files,download_opendap,load}
                        Commands to organize and retrieve data from
                        heterogenous sources"""
    assert help_string in out


def test_parsers_subset(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'subset'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Returns a netcdf file subsetted along latitudes and longitudes.

positional arguments:
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit
  --lonlatbox LONLATBOX LONLATBOX LONLATBOX LONLATBOX
                        Longitude - Latitude box in degrees. Default: 0.0
                        359.999 -90.0 90.0
  --lat_var LAT_VAR     Name of latitude variable
  --lon_var LON_VAR     Name of longitude variable
  --output_vertices     Compute and output vertices
"""
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'subset', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out


def test_parsers_validate(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'validate'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Returns anetcdf file with soft links to the data. Validates availability of
remote data.

positional arguments:
  var_name              Comma-seprated variable names to concatenate.
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit
  --file_type {local_file,OPENDAP}
                        Type of files.
  --time_var TIME_VAR   The time variable in the files. Default: time.

Time selection:
  --year YEAR           Retrieve only these comma-separated years.
  --month MONTH         Retrieve only these comma-separated months (1 to 12).
  --day DAY             Retrieve only these comma-separated calendar days.
  --hour HOUR           Retrieve only these comma-separated hours.
  --previous            Retrieve data from specified year, month, day AND the
                        time step just BEFORE this retrieved data.
  --next                Retrieve data from specified year, month, day AND the
                        time step just AFTER this retrieved data.

Use these arguments to let nc_soft_links manage your credentials:
  --openid OPENID       Pass your ESGF openid. nc_soft_links will prompt you
                        once for your password and will ensure that your
                        credentials are active for the duration of the
                        process. If your openid starts with
                        https://ceda.ac.uk, you must, in addition, pass a
                        username.
  --username USERNAME   Pass you username. Necessary for compatibility with
                        CEDA openids or for FTP queries.
  --password PASSWORD   Your ESGF password
  --password_from_pipe  If activated it is expected that the user is passing a
                        password through piping. Example: echo $PASSWORD |
                        nc_soft_links ...
  --timeout TIMEOUT     Set the time after which opendap access will timeout
                        (in seconds). If a connection is slow, TIMEOUT should
                        probably be larger. Default: 120s.
"""
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'validate', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out


def test_parsers_download_files(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_files'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Take as an input the results from 'validate' and returns a soft links file
with the HTTPServer / FTPServer and GRIDFTP data filling the database.

positional arguments:
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit
  --out_download_dir OUT_DOWNLOAD_DIR
                        Destination directory for retrieval.
  --download_all_files  Download all remote files corresponding to the
                        request, even if a local_file or an OPENDAP link are
                        available. By default, download only files that have
                        no alternatives.
  --do_not_revalidate   Do not revalidate. Only advanced users will use this
                        option. Using this option might can lead to ill-
                        defined time axes.
  --download_cache DOWNLOAD_CACHE
                        Cache file for downloads
  --num_dl NUM_DL       Number of simultaneous download from EACH data node.
                        Default=1.
  --serial              Force serial analysis.
  --time_var TIME_VAR   Name of time variable. Default=time.

Use these arguments to let nc_soft_links manage your credentials:
  --openid OPENID       Pass your ESGF openid. nc_soft_links will prompt you
                        once for your password and will ensure that your
                        credentials are active for the duration of the
                        process. If your openid starts with
                        https://ceda.ac.uk, you must, in addition, pass a
                        username.
  --username USERNAME   Pass you username. Necessary for compatibility with
                        CEDA openids or for FTP queries.
  --password PASSWORD   Your ESGF password
  --password_from_pipe  If activated it is expected that the user is passing a
                        password through piping. Example: echo $PASSWORD |
                        nc_soft_links ...
  --timeout TIMEOUT     Set the time after which opendap access will timeout
                        (in seconds). If a connection is slow, TIMEOUT should
                        probably be larger. Default: 120s.

Restrict to specific data nodes:
  --data_node DATA_NODE
                        Consider only the specified data nodes
  --Xdata_node XDATA_NODE
                        Do not consider the specified data nodes

Time selection:
  --year YEAR           Retrieve only these comma-separated years.
  --month MONTH         Retrieve only these comma-separated months (1 to 12).
  --day DAY             Retrieve only these comma-separated calendar days.
  --hour HOUR           Retrieve only these comma-separated hours.
  --previous            Retrieve data from specified year, month, day AND the
                        time step just BEFORE this retrieved data.
  --next                Retrieve data from specified year, month, day AND the
                        time step just AFTER this retrieved data.
"""

    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_files', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out


def test_parsers_download_files(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_files'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Take as an input the results from 'validate' and returns a soft links file
with the HTTPServer / FTPServer and GRIDFTP data filling the database.

positional arguments:
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit
  --out_download_dir OUT_DOWNLOAD_DIR
                        Destination directory for retrieval.
  --download_all_files  Download all remote files corresponding to the
                        request, even if a local_file or an OPENDAP link are
                        available. By default, download only files that have
                        no alternatives.
  --do_not_revalidate   Do not revalidate. Only advanced users will use this
                        option. Using this option might can lead to ill-
                        defined time axes.
  --download_cache DOWNLOAD_CACHE
                        Cache file for downloads
  --num_dl NUM_DL       Number of simultaneous download from EACH data node.
                        Default=1.
  --serial              Force serial analysis.
  --time_var TIME_VAR   Name of time variable. Default=time.

Use these arguments to let nc_soft_links manage your credentials:
  --openid OPENID       Pass your ESGF openid. nc_soft_links will prompt you
                        once for your password and will ensure that your
                        credentials are active for the duration of the
                        process. If your openid starts with
                        https://ceda.ac.uk, you must, in addition, pass a
                        username.
  --username USERNAME   Pass you username. Necessary for compatibility with
                        CEDA openids or for FTP queries.
  --password PASSWORD   Your ESGF password
  --password_from_pipe  If activated it is expected that the user is passing a
                        password through piping. Example: echo $PASSWORD |
                        nc_soft_links ...
  --timeout TIMEOUT     Set the time after which opendap access will timeout
                        (in seconds). If a connection is slow, TIMEOUT should
                        probably be larger. Default: 120s.

Restrict to specific data nodes:
  --data_node DATA_NODE
                        Consider only the specified data nodes
  --Xdata_node XDATA_NODE
                        Do not consider the specified data nodes

Time selection:
  --year YEAR           Retrieve only these comma-separated years.
  --month MONTH         Retrieve only these comma-separated months (1 to 12).
  --day DAY             Retrieve only these comma-separated calendar days.
  --hour HOUR           Retrieve only these comma-separated hours.
  --previous            Retrieve data from specified year, month, day AND the
                        time step just BEFORE this retrieved data.
  --next                Retrieve data from specified year, month, day AND the
                        time step just AFTER this retrieved data.
"""

    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_files', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out


def test_parsers_download_opendap(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_opendap'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Take as an input the results from 'validate' and returns a soft links file
with the opendap data filling the database. Must be called after
'download_files' in order to prevent missing data.

positional arguments:
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit
  --download_all_opendap
                        Download all remote opendap links, even if a
                        local_file is available. By default, download only
                        OPENDAP links that have no alternatives.
  --download_cache DOWNLOAD_CACHE
                        Cache file for downloads
  --num_dl NUM_DL       Number of simultaneous download from EACH data node.
                        Default=1.
  --serial              Force serial analysis.
  --time_var TIME_VAR   Name of time variable. Default=time.

Use these arguments to let nc_soft_links manage your credentials:
  --openid OPENID       Pass your ESGF openid. nc_soft_links will prompt you
                        once for your password and will ensure that your
                        credentials are active for the duration of the
                        process. If your openid starts with
                        https://ceda.ac.uk, you must, in addition, pass a
                        username.
  --username USERNAME   Pass you username. Necessary for compatibility with
                        CEDA openids or for FTP queries.
  --password PASSWORD   Your ESGF password
  --password_from_pipe  If activated it is expected that the user is passing a
                        password through piping. Example: echo $PASSWORD |
                        nc_soft_links ...
  --timeout TIMEOUT     Set the time after which opendap access will timeout
                        (in seconds). If a connection is slow, TIMEOUT should
                        probably be larger. Default: 120s.

Restrict to specific data nodes:
  --data_node DATA_NODE
                        Consider only the specified data nodes
  --Xdata_node XDATA_NODE
                        Do not consider the specified data nodes

Time selection:
  --year YEAR           Retrieve only these comma-separated years.
  --month MONTH         Retrieve only these comma-separated months (1 to 12).
  --day DAY             Retrieve only these comma-separated calendar days.
  --hour HOUR           Retrieve only these comma-separated hours.
  --previous            Retrieve data from specified year, month, day AND the
                        time step just BEFORE this retrieved data.
  --next                Retrieve data from specified year, month, day AND the
                        time step just AFTER this retrieved data.
"""

    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'download_opendap', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out

def test_parsers_load(capsys):
    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'load'])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err

    help_string = """
Takes as an input the results from 'validate' and loads local data into the
database. Removes soft links informations. Must be used after download_files
and download_opendap in order to prevent missing data.

positional arguments:
  in_netcdf_file        NETCDF paths file (input)
  out_netcdf_file       NETCDF retrieved file (output)

optional arguments:
  -h, --help            show this help message and exit

Specify verbosity in downloads:
  -s, --silent          Make downloads silent.

Use these arguments to let nc_soft_links manage your credentials:
  --openid OPENID       Pass your ESGF openid. nc_soft_links will prompt you
                        once for your password and will ensure that your
                        credentials are active for the duration of the
                        process. If your openid starts with
                        https://ceda.ac.uk, you must, in addition, pass a
                        username.
  --username USERNAME   Pass you username. Necessary for compatibility with
                        CEDA openids or for FTP queries.
  --password PASSWORD   Your ESGF password
  --password_from_pipe  If activated it is expected that the user is passing a
                        password through piping. Example: echo $PASSWORD |
                        nc_soft_links ...
  --timeout TIMEOUT     Set the time after which opendap access will timeout
                        (in seconds). If a connection is slow, TIMEOUT should
                        probably be larger. Default: 120s.

Time selection:
  --year YEAR           Retrieve only these comma-separated years.
  --month MONTH         Retrieve only these comma-separated months (1 to 12).
  --day DAY             Retrieve only these comma-separated calendar days.
  --hour HOUR           Retrieve only these comma-separated hours.
  --previous            Retrieve data from specified year, month, day AND the
                        time step just BEFORE this retrieved data.
  --next                Retrieve data from specified year, month, day AND the
                        time step just AFTER this retrieved data.
"""

    with pytest.raises(SystemExit):
        parsers.full_parser(['nc4sl', 'load', '-h'])
    out, err = capsys.readouterr()
    assert help_string in out
