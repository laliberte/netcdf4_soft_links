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

def download_raw(options,manager,semaphores):
    remote_retrieve_and_download(options,manager,retrieve_type='raw')
    return

def download(options,manager,semaphores):
    remote_retrieve_and_download(options,manager,retrieve_type='data')
    return

def remote_retrieve_and_download(options,manager,retrieve_type='data'):

    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    data=netCDF4.Dataset(options.in_netcdf_file,'r')
    data_node_list=list(set(data.groups['soft_links'].variables['data_node'][:]))
    #manager=multiprocessing.Manager()
    queues, processes=retrieval_manager.start_processes(options,data_node_list,manager=manager)

    try:
        netcdf_pointers=read_soft_links.read_netCDF_pointers(data,options=options,queue=queues['download_start'])
        if retrieve_type=='data':
            netcdf_pointers.retrieve(output,'retrieve_path_data',filepath=options.out_netcdf_file)
        elif retrieve_type=='raw':
            netcdf_pointers.retrieve(output,'retrieve_path',filepath=options.out_netcdf_file,out_dir=options.out_destination)

        retrieval_manager.launch_download_and_remote_retrieve(output,data_node_list,queues,options)
    finally:
        for item in processes.keys():
            processes[item].terminate()
    return
