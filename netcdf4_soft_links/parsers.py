#External:
import argparse 
import textwrap
import datetime
import copy

def input_arguments(parser):
    parser.add_argument('in_netcdf_file',
                     help='NETCDF paths file (input)')
    return

def output_arguments(parser):
    parser.add_argument('out_netcdf_file',
                     help='NETCDF retrieved file (output)')
    return

def generate_subparsers(parser,epilog,project_drs):
    subparsers = parser.add_subparsers(help='Commands to organize and retrieve data from heterogenous sources',dest='command')
    certificates(subparsers,epilog,project_drs)
    subset(subparsers,epilog,project_drs)

    validate(subparsers,epilog,project_drs)
    download_files(subparsers,epilog,project_drs)
    download_opendap(subparsers,epilog,project_drs)
    load(subparsers,epilog,project_drs)
    return

def subset(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('subset',
               description=textwrap.dedent('Returns a\n\
                     netcdf file subsetted along latitudes and longitudes.'),
               epilog=epilog_validate,
             )
    parser.add_argument('--lonlatbox',default=[0.0,359.999,-90.0,90.0],
                      nargs=4,type=float,
                     help='Longitude - Latitude box in degrees. Default: 0.0 359.999 -90.0 90.0')
    parser.add_argument('--lat_var',default='lat',
                     help='Name of latitude variable')
    parser.add_argument('--lon_var',default='lon',
                     help='Name of longitude variable')
    parser.add_argument('--output_vertices',action='store_true',help='Compute and output vertices')
    input_arguments(parser)
    output_arguments(parser)
    return

def validate(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('validate',
               description=textwrap.dedent('Returns a\n\
                     netcdf file with soft links to the data. Validates availability of remote data.'),
               epilog=epilog_validate,
             )
    validate_arguments(parser,project_drs)
    certificates_arguments(parser,project_drs)
    return

def validate_arguments(parser,project_drs):
    parser.add_argument('var_name',
                     default=[], type=str_list,
                     help='Comma-seprated variable names to concatenate.')
    parser.add_argument('in_netcdf_file',nargs='*',
                     help='NETCDF paths file (input)')
    output_arguments(parser)

    time_selection_arguments(parser,project_drs)
    parser.add_argument('--file_type',default='local_file',choices=['local_file','OPENDAP'],
                     help='Type of files.')
    parser.add_argument('--time_var',default='time',
                     help='The time variable in the files. Default: time.')

    return

def download_files(subparsers,epilog,project_drs):
    epilog_download_files=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download_files',
                   description=textwrap.dedent('Take as an input the results from \'validate\' and returns a\n\
                                                soft links file with the HTTPServer / FTPServer and GRIDFTP data filling the database.'),
                   epilog=epilog_download_files,
                 )
    download_files_arguments(parser,project_drs)
    parser.add_argument('--time_var',default='time',help='Name of time variable. Default=time.')
    return parser

def download_files_arguments(parser,project_drs):
    input_arguments(parser)
    output_arguments(parser)
    download_files_arguments_no_io(parser,project_drs)
    download_arguments_no_io(parser,project_drs)
    serial_arguments(parser,project_drs)
    certificates_arguments(parser,project_drs)
    data_node_restriction(parser,project_drs)
    time_selection_arguments(parser,project_drs)
    return parser

def download_opendap(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download_opendap',
               description=textwrap.dedent('Take as an input the results from \'validate\' and returns a\n\
                                            soft links file with the opendap data filling the database.\n\
                                            Must be called after \'download_files\' in order to prevent missing data.'),
               epilog=epilog_validate,
             )
    download_opendap_arguments(parser,project_drs)
    parser.add_argument('--time_var',default='time',help='Name of time variable. Default=time.')
    return parser

def download_opendap_arguments(parser,project_drs):
    input_arguments(parser)
    output_arguments(parser)
    download_opendap_arguments_no_io(parser,project_drs)
    download_arguments_no_io(parser,project_drs)
    serial_arguments(parser,project_drs)
    certificates_arguments(parser,project_drs)
    data_node_restriction(parser,project_drs)
    time_selection_arguments(parser,project_drs)
    return parser

def download_opendap_arguments_no_io(parser,project_drs):
    parser.add_argument('--download_all_opendap',default=False,action='store_true',
                        help='Download all remote opendap links, even if a local_file is available.\n\
                              By default, download only OPENDAP links that have no alternatives.')
    return

def download_files_arguments_no_io(parser,project_drs):
    if project_drs==None:
        default_dir='.'
    else:
        default_dir='./'+project_drs.project
    parser.add_argument('--out_download_dir',default='.',
                             help='Destination directory for retrieval.')
    parser.add_argument('--download_all_files',default=False,action='store_true',
                        help='Download all remote files corresponding to the request, even if a local_file or an OPENDAP link are available.\n\
                              By default, download only files that have no alternatives.')

    parser.add_argument('--do_not_revalidate',default=False,action='store_true',
                        help='Do not revalidate. Only advanced users will use this option.\n\
                              Using this option might can lead to ill-defined time axes.')
    return 

def download_arguments_no_io(parser,project_drs):
    parser.add_argument('--download_cache',help='Cache file for downloads')
    parser.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')
    return parser

def serial_arguments(parser,project_drs):
    parser.add_argument('--serial',default=False,action='store_true',help='Force serial analysis.')
    return parser

def load(subparsers,epilog,project_drs):
    epilog_load=textwrap.dedent(epilog)
    parser=subparsers.add_parser('load',
                   description=textwrap.dedent('Take as an input the results from \'validate\' and loads local data into the database.\n\
                                                Removes soft links informations. Must be used after download_files and download_opendap in order\n\
                                                to prevent missing data.'),
                   epilog=epilog_load,
                 )
    input_arguments(parser)
    output_arguments(parser)
    verbosity_group = parser.add_argument_group('Specify verbosity in downloads')
    verbosity_group.add_argument('-s','--silent',default=False,action='store_true',help='Make downloads silent.')
    certificates_arguments(parser,project_drs)
    time_selection_arguments(parser,project_drs)
    return parser

def certificates(subparsers,epilog,project_drs):
    epilog_certificates=textwrap.dedent(epilog)
    parser=subparsers.add_parser('certificates',
                           description=textwrap.dedent('Recovers ESGF certificates. This method of obtaining credentials \n\
                                                        has been superseded by the more robust cookie method in netcdf4_soft_links version 0.6 \n\
                                                        Will most likely become deprecated in the near future. '),
                           epilog=epilog_certificates
                         )
    certificates_arguments(parser,project_drs)
    return

def time_selection_arguments(parser,project_drs):
    time_inc_group = parser.add_argument_group('Time selection')
    time_inc_group.add_argument('--year',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated years.')
    time_inc_group.add_argument('--month',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated months (1 to 12).')
    time_inc_group.add_argument('--day',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated calendar days.')
    time_inc_group.add_argument('--hour',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated hours.')
    time_inc_group.add_argument('--previous',
                                 default=0,action='count',
                                 help='Retrieve data from specified year, month, day AND the time step just BEFORE this retrieved data.')
    time_inc_group.add_argument('--next',
                                 default=0,action='count',
                                 help='Retrieve data from specified year, month, day AND the time step just AFTER this retrieved data.')
    return

def certificates_arguments(parser,project_drs):
    if project_drs==None:
        script_name='nc_soft_links'
    else:
        script_name='cdb_query '+project_drs.project

    cert_group = parser.add_argument_group('Use these arguments to let cdb_query manage your credentials')
    cert_group.add_argument('--openid',default=None,
                     help='Pass your ESGF openid.\n\
                           cdb_query will prompt you once for your password and will ensure that your credentials \n\
                           are active for the duration of the process. If your openid starts with https://ceda.ac.uk, you\n\
                           must, in addition, pass a username.')
    cert_group.add_argument('--username',default=None,
                            help='Pass you username. Necessary for compatibility with CEDA openids or for \n\
                             FTP queries.')
    cert_group.add_argument('--password', help='Your ESGF password')
    cert_group.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASSWORD | '+script_name+' ...')
    cert_group.add_argument('--timeout',default=120,type=int,
                     help='Set the time after which opendap access will timeout (in seconds).\n\
                           If a connection is slow, TIMEOUT should probably be larger. Default: 120s.')

    cert_group.add_argument('--use_certificates',default=False,action='store_true',
                     help='Use certificates. Only valid with a CEDA openid (e.g. https://ceda.ac.uk/openid/OPENID). \n\
                           Will likely be deprecated in future versions.')
    cert_group.add_argument('--no_trustroots',default=True,action='store_false',
                     help='Bypass trustroots retrieval. Retrieval can be slow and is necessary only one in a while. \n\
                           Will liekly be deprecated in future versions.')
    return

def data_node_restriction(parser,project_drs):
    data_node_group = parser.add_argument_group('Restrict to specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Consider only the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not consider the specified data nodes')
    return

def int_list(input):
    return [ int(item) for item in input.split(',')]

def str_list(input):
    if len(input.split(','))==1:
        return input
    else:
        return [ item for item in input.split(',')]
