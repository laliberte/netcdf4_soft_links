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
    #Discover tree
    subparsers = parser.add_subparsers(help='Commands to discover available data on the archive',dest='command')

    #Optimset tree
    validate(subparsers,epilog,project_drs)
    download_files(subparsers,epilog,project_drs)
    download_opendap(subparsers,epilog,project_drs)
    load(subparsers,epilog,project_drs)
    certificates(subparsers,epilog,project_drs)
    return

def validate(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('validate',
               description=textwrap.dedent('Returns a\n\
                     netcdf file with soft links to the data. Validates availability of remote data.'),
               epilog=epilog_validate,
             )
    validate_arguments(parser,project_drs)
    return

def validate_arguments(parser,project_drs):
    parser.add_argument('var_name',
                     default=[], type=str_list,
                     help='Comma-seprated variable names to concatenate.')

    parser.add_argument('in_netcdf_file',nargs='+',
                     help='NETCDF files (input)')

    parser.add_argument('out_netcdf_file',
                     help='NETCDF file (output)')

    parser.add_argument('--file_type',default='local_file',choices=['local_file','OPENDAP'],
                     help='Type of files.')

    time_inc_group = parser.add_argument_group('Time selection')
    time_inc_group.add_argument('--year',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated years.')
    time_inc_group.add_argument('--month',
                                 default=None, type=int_list,
                                 help='Retrieve only these comma-separated months (1 to 12). \n\
                                       If the list of months is composed only of continuous sublists (e.g. 1,2,12)\n\
                                       it ensures that continuous months are retrieved.')

    certificates_arguments(parser,project_drs)
    return

def download_opendap(subparsers,epilog,project_drs):
    epilog_validate=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download_opendap',
               description=textwrap.dedent('Take as an input the results from \'validate\' and returns a\n\
                                            soft links file with the opendap data filling the database.'),
               epilog=epilog_validate,
             )
    download_opendap_arguments(parser,project_drs)
    return parser

def download_opendap_arguments(parser,project_drs):
    input_arguments(parser)
    output_arguments(parser)
    download_arguments_no_io(parser,project_drs)
    return parser

def download_files(subparsers,epilog,project_drs):
    epilog_download_files=textwrap.dedent(epilog)
    parser=subparsers.add_parser('download_files',
                   description=textwrap.dedent('Take as an input the results from \'validate\' and returns a\n\
                                                soft links file with the HTTPServer / FTPServer and GRIDFTP data filling the database.'),
                   epilog=epilog_download_files,
                 )
    download_files_arguments(parser,project_drs)
    return parser

def download_files_arguments(parser,project_drs):
    if project_drs==None:
        default_dir='.'
    else:
        default_dir='./'+project_drs.project
    parser.add_argument('--out_destination',default='.',
                             help='Destination directory for retrieval.')
    input_arguments(parser)
    output_arguments(parser)
    download_arguments_no_io(parser,project_drs)
    return parser

def download_arguments_no_io(parser,project_drs):
    serial_group=load_arguments_no_io(parser,project_drs)
    serial_group.add_argument('--num_dl',default=1,type=int,help='Number of simultaneous download from EACH data node. Default=1.')

    data_node_group = parser.add_argument_group('Limit download from specific data nodes')
    data_node_group.add_argument('--data_node',type=str,action='append',help='Retrieve only from the specified data nodes')
    data_node_group.add_argument('--Xdata_node',type=str,action='append',help='Do not retrieve from the specified data nodes')
    return parser

def load(subparsers,epilog,project_drs):
    epilog_load=textwrap.dedent(epilog)
    parser=subparsers.add_parser('load',
                   description=textwrap.dedent('Take as an input the results from \'validate\' and loads local data into the database.\n\
                                                Removes soft links informations. Must be used after download_files and download_opendap in order\n\
                                                to prevent missing data.'),
                   epilog=epilog_load,
                 )
    load_arguments_no_io(parser,project_drs)
    return parser

def load_arguments_no_io(parser,project_drs):
    certificates_arguments(parser,project_drs)

    verbosity_group = parser.add_argument_group('Specify verbosity in downloads')
    verbosity_group.add_argument('-s','--silent',default=False,action='store_true',help='Make downloads silent.')

    serial_group = parser.add_argument_group('Specify asynchronous behavior')
    serial_group.add_argument('--serial',default=False,action='store_true',help='Downloads the files serially.')

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
    return serial_group

def certificates(subparsers,epilog,project_drs):
    epilog_certificates=textwrap.dedent(epilog)
    parser=subparsers.add_parser('certificates',
                           description=textwrap.dedent('Recovers ESGF certificates'),
                           epilog=epilog_certificates
                         )
    certificates_arguments(parser,project_drs)
    return

def certificates_arguments(parser,project_drs):
    if project_drs==None:
        script_name='nc_soft_links'
    else:
        script_name='cdb_query_'+project_drs.project

    cert_group = parser.add_argument_group('Use these arguments to let cdb_query manage your credentials')
    cert_group.add_argument('--username',default=None,
                     help='If you set this value to your user name for registering service given in --service \n\
                           cdb_query will prompt you once for your password and will ensure that your credentials \n\
                           are active for the duration of the process.')
    cert_group.add_argument('--password_from_pipe',default=False,action='store_true',
                        help='If activated it is expected that the user is passing a password through piping\n\
                              Example: echo $PASS | '+script_name+' ...')
    cert_group.add_argument('--service',default='badc',choices=['badc'],
                     help='Registering service. At the moment works only with badc.')
    cert_group.add_argument('--no_trustroots',default=True,action='store_false',
                     help='Bypass trustroots retrieval.')
    return

def int_list(input):
    return [ int(item) for item in input.split(',')]

def str_list(input):
    if len(input.split(','))==1:
        return input
    else:
        return [ item for item in input.split(',')]
