#External
import datetime
import os
import netCDF4
import socket
import multiprocessing

#Internal
import create_soft_links
import read_soft_links
import retrieval_manager
import queues_manager


valid_file_type_list=['local_file','OPENDAP']
time_frequency=None
unique_file_id_list=['checksum_type','checksum','tracking_id']
drs_to_pass=['path','version','file_type','data_node']

def validate(options,queues,semaphores):
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

    netcdf_pointers=create_soft_links.create_netCDF_pointers(
                                                      paths_list,
                                                      time_frequency,options.year,options.month,
                                                      valid_file_type_list,
                                                      list(set(data_node_list)),
                                                      record_other_vars=False,
                                                      semaphores=validate_semaphores)
    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    netcdf_pointers.record_meta_data(output,options.var_name)
    return

def download_files(options):
    download(options,manager,retrieval_type='download_files')
    return

def download_opendap(options):
    download(options,manager,retrieval_type='download_opendap')
    return

def load(options):
    download(options,manager,retrieval_type='load')
    return

def download(options,manager,retrieval_type='load'):

    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    data=netCDF4.Dataset(options.in_netcdf_file,'r')
    data_node_list=list(set(data.groups['soft_links'].variables['data_node'][:]))

    if retrieval_type!='load':
        #Create manager:
        processes_names=[multiprocessing.current_process().name,]
        q_manager=queues_manager.NC4SL_queues_manager(options,processes_names,manager=manager)

        #Create download queues:
        for data_node in data_node_list:
            q_manager.semaphores.add_new_data_node(data_node)
            q_manager.queues.add_new_data_node(data_node)

        download_processes=retrieval_manager.start_download_processes(q_manager,options)

    try:
        q_manager.set_opened()
        netcdf_pointers=read_soft_links.read_netCDF_pointers(data,options=options,semaphores=q_manager.semaphores,queues=q_manager)
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
