#External:
import multiprocessing
import datetime
#import sys
#from StringIO import StringIO
import netCDF4

#Internal:
import netcdf_utils
import remote_netcdf
import certificates
import retrieval_utils

def start_processes(options,data_node_list,manager=None):
    queues=define_queues(options,data_node_list,manager)
    #Redefine data nodes:

    processes=dict()
    if not ('serial' in dir(options) and options.serial):
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                process_name=data_node+'-'+str(simultaneous_proc)
                processes[process_name]=multiprocessing.Process(target=worker_retrieve, 
                                                name=process_name,
                                                args=(queues,data_node))
                processes[process_name].start()
        process_name='download_staging'
        processes[process_name]=multiprocessing.Process(target=worker_stage, 
                                        name=process_name,
                                        args=(queues,))
        processes[process_name].start()
    return queues, processes

def worker_retrieve(queues,data_node):
    for item in iter(queues[data_node].get, 'STOP'):
        result = function_retrieve(item)
        queues['download_end'].put(result)
    queues[data_node].put('STOP')
    queues['download_end'].put('STOP')
    return

def function_retrieve(item):
    return item[0](item[1],item[2])

def worker_stage(queues):
    for item in iter(queues['download_start'].get,'STOP'):
        queues[item[1]['data_node']].put(item)
    return

def worker_exit(queues,queues_size,start_time,renewal_time,output,options):
    for item in iter(queues['download_end'].get,'STOP'):
        renewal_time=progress_report(item,queues,queues_size,start_time,renewal_time,output,options)
    return renewal_time

def launch_download_and_remote_retrieve(output,data_node_list,queues,options):
    #Second step: Process the queues:
    #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = datetime.datetime.now()
    renewal_time = datetime.datetime.now()
    queues_size=dict()
    if 'silent' in dir(options) and not options.silent:
        print('Remaining retrieval from data nodes:')
        print queues
        for data_node in data_node_list:
            queues_size[data_node]=queues[data_node].qsize()
        string_to_print=['0'.zfill(len(str(queues_size[data_node])))+'/'+str(queues_size[data_node])+' paths from "'+data_node+'"' for
                            data_node in data_node_list]
        print ' | '.join(string_to_print)
        print 'Progress: '

    if 'serial' in dir(options) and options.serial:
        queues['download_start'].put('STOP')
        worker_stage(queues)
        for data_node in data_node_list:
            queues[data_node].put('STOP')
            worker_retrieve(queues,data_node)
            renewal_time=worker_exit(queues,queues_size,start_time,renewal_time,output,options)
    else:
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                queues[data_node].put('STOP')
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                renewal_time=worker_exit(queues,queues_size,start_time,renewal_time,output,options)
                
    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        output.close()

    if 'silent' in dir(options) and not options.silent:
        print
        print('Done!')
    return

def progress_report(item,queues,queues_size,start_time,renewal_time,output,options):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time
    data_node_list=queues.keys()
    data_node_list.remove('download_start')
    data_node_list.remove('download_end')

    if item==retrieval_utils.retrieve_path_data:
        netcdf_utils.assign_tree(output,*item)
        output.sync()
        if 'silent' in dir(options) and not options.silent:
            string_to_print=[str(queues_size[data_node]-queues[data_node].qsize()).zfill(len(str(queues_size[data_node])))+
                             '/'+str(queues_size[data_node]) for
                                data_node in data_node_list]
            print str(elapsed_time)+', '+' | '.join(string_to_print)+'\r',
    else:
        if 'silent' in dir(options) and not options.silent:
            #print '\t', queues['end'].get()
            if item!=None:
                print '\t', item
                print str(elapsed_time)

    #Maintain certificates:
    if ('username' in dir(options) and 
        options.username!=None and
        options.password!=None and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
        renewal_time=datetime.datetime.now()
    return renewal_time

def define_queues(options,data_node_list,manager):
    #Define queues if there no queues already defined:
    if manager==None:
        queues={data_node : multiprocessing.Queue() for data_node in data_node_list}
        queues['download_start']= multiprocessing.Queue()
        queues['download_end']= multiprocessing.Queue()
    else:
        queues={data_node : manager.Queue() for data_node in data_node_list}
        queues['download_start']= manager.Queue()
        queues['download_end']= manager.Queue()
    return queues

#class MyStringIO(StringIO):
#    def __init__(self, queue, *args, **kwargs):
#        StringIO.__init__(self, *args, **kwargs)
#        self.queue = queue
#    def flush(self):
#        self.queue.put((multiprocessing.current_process().name, self.getvalue()))
#        self.truncate(0)
#
#def initializer(queue):
#     sys.stderr = sys.stdout = MyStringIO(queue)
