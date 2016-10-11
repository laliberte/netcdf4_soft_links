#External
import datetime
import os
import netCDF4
import socket
import multiprocessing
import requests

#Internal
from .soft_links import create_soft_links, read_soft_links
from .subset import subset_utils
from .remote_netcdf import remote_netcdf
from . import retrieval_manager, queues_manager

valid_file_type_list=['local_file','OPENDAP']
time_frequency=None
unique_file_id_list=['checksum_type','checksum','tracking_id']
drs_to_pass=['path','version','file_type','data_node']

def validate(options):
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

    manager=multiprocessing.Manager()
    validate_semaphores=queues_manager.Semaphores_data_node(manager,num_concurrent=5)

    remote_netcdf_kwargs={opt: getattr(options,opt) for opt in ['openid','username','password','use_certificates',
                                                                 ] if opt in dir(options)}
    session=requests.Session()
    netcdf_pointers=create_soft_links.create_netCDF_pointers(
                                                      paths_list,
                                                      time_frequency,options.year,options.month,
                                                      valid_file_type_list,
                                                      list(set(data_node_list)),
                                                      record_other_vars=False,
                                                      semaphores=validate_semaphores,
                                                      time_var=options.time_var,
                                                      session=session,
                                                      remote_netcdf_kwargs=remote_netcdf_kwargs)
    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    netcdf_pointers.record_meta_data(output,options.var_name)
    return

def download_files(options):
    download(options,retrieval_type='download_files')
    return

def download_opendap(options):
    download(options,retrieval_type='download_opendap')
    return

def load(options):
    download(options,retrieval_type='load')
    return

def download(options,retrieval_type='load'):

    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    data=netCDF4.Dataset(options.in_netcdf_file,'r')
    data_node_list=list(set(data.groups['soft_links'].variables['data_node'][:]))

    if retrieval_type!='load':
        #Create manager:
        processes_names=[multiprocessing.current_process().name,]
        q_manager=queues_manager.NC4SL_queues_manager(options,processes_names)

        #Create download queues:
        for data_node in data_node_list:
            q_manager.semaphores.add_new_data_node(data_node)
            q_manager.queues.add_new_data_node(data_node)

        download_processes=retrieval_manager.start_download_processes(options,q_manager)

    try:
        q_manager.set_opened()
        remote_netcdf_kwargs={opt: getattr(options,opt) for opt in ['openid','username','password','use_certificates',
                                                                     ] if opt in dir(options)}
        session=requests.Session()
        options_dict={opt: getattr(options,opt) for opt in ['previous','next','year','month','day','hour',
                                                                     'download_all_files','download_all_opendap'] if opt in dir(options)}
        options_dict['remote_netcdf_kwargs']=remote_netcdf_kwargs

        netcdf_pointers=read_soft_links.read_netCDF_pointers(data,time_var=options.time_var,
                                                                  q_manager=q_manager,
                                                                  session=session,
                                                                  **options_dict)
        if retrieval_type=='download_files':
            netcdf_pointers.retrieve(output,retrieval_type,filepath=options.out_netcdf_file,out_dir=options.out_download_dir)
        else:
            netcdf_pointers.retrieve(output,retrieval_type,filepath=options.out_netcdf_file)

        if retrieval_type!='load':
            #Close queues:
            q_manager.set_closed()
            output=retrieval_manager.launch_download(output,data_node_list,q_manager,options)
            output.close()
            if ( retrieval_type=='download_files' and
                not ( 'do_not_revalidate' in dir(options) and options.do_not_revalidate)):
                pass
                #Revalidate not implemented yet
    finally:
        if retrieval_type!='load':
            #Terminate the download processes:
            for item in download_processes.keys():
                download_processes[item].terminate()
    return

def subset(options):
    subset_utils.subset(options.in_netcdf_file,options.out_netcdf_file,
                            lonlatbox=options.lonlatbox,
                            lat_var=options.lat_var,lon_var=options.lon_var,
                            output_vertices=options.output_vertices)
    return
