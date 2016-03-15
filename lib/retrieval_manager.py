#External:
import multiprocessing
import datetime
import sys
from StringIO import StringIO
import netCDF4

#Internal:
import netcdf_utils
import remote_netcdf
import certificates

def start_processes(options,data_node_list,queues=dict(),manager=None):
    queues=define_queues(options,data_node_list,queues,manager)
    #Redefine data nodes:
    data_node_list=queues.keys()
    data_node_list.remove('end')

    if not ('serial' in dir(options) and options.serial):
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                process=multiprocessing.Process(target=worker_retrieve, 
                                                name=data_node+'-'+str(simultaneous_proc),
                                                args=(queues[data_node], queues['end']))
                process.start()
    return queues, data_node_list

class MyStringIO(StringIO):
    def __init__(self, queue, *args, **kwargs):
        StringIO.__init__(self, *args, **kwargs)
        self.queue = queue
    def flush(self):
        self.queue.put((multiprocessing.current_process().name, self.getvalue()))
        self.truncate(0)

def initializer(queue):
     sys.stderr = sys.stdout = MyStringIO(queue)

def worker_retrieve(input, output):
    for tuple in iter(input.get, 'STOP'):
        result = tuple[0](tuple[1],tuple[2])
        output.put(result)
    output.put('STOP')
    return

def launch_download_and_remote_retrieve(output,data_node_list,queues,options):
    #Second step: Process the queues:
    #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = datetime.datetime.now()
    renewal_time = datetime.datetime.now()
    if 'silent' in dir(options) and not options.silent:
        print('Remaining retrieval from data nodes:')

    queues_size=dict()
    for data_node in data_node_list:
        queues_size[data_node]=queues[data_node].qsize()
    string_to_print=['0'.zfill(len(str(queues_size[data_node])))+'/'+str(queues_size[data_node])+' paths from "'+data_node+'"' for
                        data_node in data_node_list]
    if 'silent' in dir(options) and not options.silent:
        print ' | '.join(string_to_print)
        print 'Progress: '

    if 'serial' in dir(options) and options.serial:
        for data_node in data_node_list:
            queues[data_node].put('STOP')
            worker_retrieve(queues[data_node], queues['end'])
            for tuple in iter(queues['end'].get, 'STOP'):
                renewal_time=progress_report(options,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time)
        
    else:
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                queues[data_node].put('STOP')
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                for tuple in iter(queues['end'].get, 'STOP'):
                    renewal_time=progress_report(options,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time)

    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        output.close()

    if 'silent' in dir(options) and not options.silent:
        print
        print('Done!')
        #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return

def progress_report(options,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time
    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        netcdf_utils.assign_tree(output,*tuple)
        output.sync()
        string_to_print=[str(queues_size[data_node]-queues[data_node].qsize()).zfill(len(str(queues_size[data_node])))+
                         '/'+str(queues_size[data_node]) for
                            data_node in data_node_list]
        if 'silent' in dir(options) and not options.silent:
            print str(elapsed_time)+', '+' | '.join(string_to_print)+'\r',
    else:
        if 'silent' in dir(options) and not options.silent:
            #print '\t', queues['end'].get()
            if tuple!=None:
                print '\t', tuple
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

def define_queues(options,data_node_list,queues,manager):
    #Define queues if there no queues already defined:
    if not queues:
        if manager==None:
            queues={data_node : multiprocessing.Queue() for data_node in data_node_list}
            queues['end']= multiprocessing.Queue()
            if 'source_dir' in dir(options) and options.source_dir!=None:
                queues[remote_netcdf.get_data_node(options.source_dir,'local_file')]=multiprocessing.Queue()
        else:
            queues={data_node : manager.Queue() for data_node in data_node_list}
            queues['end']= manager.Queue()
            if 'source_dir' in dir(options) and options.source_dir!=None:
                queues[remote_netcdf.get_data_node(options.source_dir,'local_file')]=manager.Queue()
    return queues

