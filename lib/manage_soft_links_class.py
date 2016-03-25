import datetime
import os
import netCDF4
import socket

import remote_netcdf
import create_soft_links
import read_soft_links
import retrieval_manager

valid_file_type_list=['local_file','OPENDAP']
time_frequency=None
unique_file_id_list=['checksum_type','checksum','tracking_id']
drs_to_pass=['path','version','file_type','data_node']

def create(options,queues,semaphores):
    if 'username' in dir(options) and options.username!=None:
        if not options.password_from_pipe:
            user_pass=getpass.getpass('Enter Credential phrase:')
        else:
            user_pass=sys.stdin.readline().rstrip()
        #Get certificates if requested by user:
        certificates.retrieve_certificates(options.username,options.service,user_pass=user_pass,trustroots=options.no_trustroots)
    else:
        user_pass=None

    options.user_pass=user_pass

    #input_paths=['~/data/NASA-GMAO/MERRA/amip/6hr/atmos/6hrLev/r1i1p1/v20130706/ta/ta_6hrLev_MERRA_amip_r1i1p1_197907010000-197910311800.nc']
    input_paths=options.in_netcdf_file
    version=datetime.datetime.now().strftime('%Y%m%d')

    if options.file_type=='local_file':
        hostname = socket.gethostname()
        input_paths=[ ':'.join([hostname,os.path.abspath(os.path.expanduser(os.path.expandvars(path)))]) for path in input_paths ]
    data_node_list=[remote_netcdf.get_data_node(path,options.file_type) for path in input_paths]

    paths = zip([ '|'.join([path,]+['' for id in unique_file_id_list]) for path in input_paths],
                [version for path in input_paths],
                [options.file_type for path in input_paths],
                data_node_list)
    paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_pass)} for path in paths]

    netcdf_pointers=create_soft_links.create_netCDF_pointers(
                                                      paths_list,
                                                      time_frequency,options.year,options.month,
                                                      valid_file_type_list,
                                                      list(set(data_node_list)),
                                                      record_other_vars=False,
                                                      semaphores=semaphores)
    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    netcdf_pointers.record_meta_data(output,options.var_name)
    return

def download_raw(options,queues,semaphores):
    output=options.out_destination
    #retrieval_function_name='retrieve_path'
    #remote_retrieve_and_download(options,output,retrieval_function_name)
    remote_retrieve_and_download(options,output,queues)
    return

def download(options,queues,semaphores):
    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    #retrieval_function_name='retrieve_path_data'
    #remote_retrieve_and_download(options,output,retrieval_function_name)
    remote_retrieve_and_download(options,output,queues)
    return

def remote_retrieve_and_download(options,output,queues):
    if 'username' in dir(options) and options.username!=None:
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)

    data=netCDF4.Dataset(options.in_netcdf_file,'r')
    data_node_list=list(set(data.groups['soft_links'].variables['data_node'][:]))
    #manager=multiprocessing.Manager()
    queues, data_node_list,processes=retrieval_manager.start_processes(options,data_node_list,queues=queues)

    try:
        netcdf_pointers=read_soft_links.read_netCDF_pointers(data,options=options,queues=queues)
        #netcdf_pointers.initialize_retrieval()
        #netcdf_pointers.retrieve(output,retrieval_function_name,options,username=options.username,user_pass=options.user_pass)
        if (isinstance(output,netCDF4.Dataset) or
            isinstance(output,netCDF4.Group)):
            netcdf_pointers.retrieve(output,'retrieve_path_data',options,username=options.username,user_pass=options.password)
        else:
            netcdf_pointers.retrieve(output,'retrieve_path',options,username=options.username,user_pass=options.password)
        #netcdf_pointers.retrieve(output,options,username=options.username,user_pass=options.user_pass)

        retrieval_manager.launch_download_and_remote_retrieve(output,data_node_list,queues,options)
    finally:
        for item in processes.keys():
            processes[item].terminate()
    return
    
