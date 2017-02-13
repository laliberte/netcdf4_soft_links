# External:
import multiprocessing
import datetime
import requests
import requests_cache

# Internal:
from . import requests_sessions
from .remote_netcdf import remote_netcdf


def setup_download_processes(options):
    remote_netcdf_kwargs = dict()
    if (hasattr(options, 'download_cache') and
       options.download_cache):
        remote_netcdf_kwargs['cache'] = options.download_cache.split(',')[0]
        if len(options.download_cache.split(',')) > 1:
            (remote_netcdf_kwargs
             ['expire_after']) = (datetime
                                  .timedelta(hours=float(options
                                                         .download_cache
                                                         .split(',')[1])))

    # Add credentials:
    remote_netcdf_kwargs.update({opt: getattr(options, opt)
                                 for opt in ['openid', 'username', 'password',
                                             'use_certificates']
                                 if hasattr(options, opt)})
    # This allows time variables with different names:
    time_var = _get_time_var(options)
    return remote_netcdf_kwargs, time_var


def _get_time_var(options):
    if hasattr(options, 'time_var') and options.time_var:
        time_var = options.time_var
    else:
        time_var = 'time'
    return time_var


def start_download_processes(options, q_manager,
                             previous_processes=dict()):
    # Start processes for download. Can be run iteratively for an update.
    processes = previous_processes
    if not (hasattr(options, 'serial') and options.serial):
        remote_netcdf_kwargs, time_var = setup_download_processes(options)

        processes = start_download_processes_no_serial(
                            q_manager, options.num_dl, processes,
                            time_var=time_var,
                            remote_netcdf_kwargs=remote_netcdf_kwargs)
    return processes


def start_download_processes_no_serial(q_manager, num_dl, processes,
                                       time_var='time',
                                       remote_netcdf_kwargs=dict()):
    for data_node in q_manager.queues.keys():
        for simultaneous_proc in range(num_dl):
            process_name = data_node + '-' + str(simultaneous_proc)
            if process_name not in processes:
                processes[process_name] = (multiprocessing
                                           .Process(
                                                target=worker_retrieve,
                                                name=process_name,
                                                args=(q_manager,
                                                      data_node,
                                                      time_var,
                                                      remote_netcdf_kwargs)))
                processes[process_name].start()
    return processes


def stop_download_processes(processes):
    for proc_name in processes:
        processes[proc_name].terminate()
    return processes


def worker_retrieve(q_manager, data_node, time_var, remote_netcdf_kwargs):
    if (hasattr(q_manager, 'session') and
        (isinstance(q_manager.session, requests.Session) or
         isinstance(q_manager.session, requests_cache.core.CachedSession))):
        session = q_manager.session
    else:
        # Create one session per worker:
        session = (requests_sessions
                   .create_single_session(**remote_netcdf_kwargs))

    # Loop indefinitely. Worker will be terminated by main process or STOP
    # will be sent:
    while True:
        item = q_manager.queues.get(data_node)
        if item == 'STOP':
            break
        # try:
        thread_id = item[0]
        # trial = item[1]
        path_to_retrieve = item[2]
        file_type = item[3]
        remote_data = (remote_netcdf
                       .remote_netCDF(path_to_retrieve, file_type,
                                      session=session,
                                      time_var=time_var,
                                      **remote_netcdf_kwargs))

        var_to_retrieve = item[4]
        pointer_var = item[5]
        result = (remote_data
                  .download(var_to_retrieve, pointer_var,
                            download_kwargs=item[-1]))
        q_manager.put_for_thread_id(thread_id, (file_type, result))
        # except Exception:
        #     if trial == 3:
        #         print('Download failed with arguments ', item)
        #         #q_manager.put_for_thread_id(thread_id, (file_type, 'FAIL'))
        #         raise
        #     else:
        #         # Put back in the queue. Do not raise.
        #         # Simply put back in the queue so that failure
        #         # cannnot occur while working downloads work:
        #         item_new = (trial + 1, path_to_retrieve, file_type,
        #                     var_to_retrieve, pointer_var, item[-1])
        #         q_manager.put_again_to_data_node_from_thread_id(thread_id,
        #                                                         data_node,
        #                                                         item_new)
    return


def worker_exit(q_manager, data_node_list, queues_size, start_time,
                renewal_time, output, options):
    failed = False
    while True:
        item = q_manager.get_for_thread_id()
        if item == 'STOP':
            break
        renewal_time, failed = progress_report(item[0], item[1], q_manager,
                                               data_node_list, queues_size,
                                               start_time, renewal_time,
                                               failed, output, options)
    return renewal_time, failed


def launch_download(output, data_node_list, q_manager, options):
    start_time = datetime.datetime.now()
    renewal_time = start_time
    queues_size = dict()

    for data_node in data_node_list:
        queues_size[data_node] = q_manager.queues.qsize(data_node)
    string_to_print = ['0'.zfill(len(str(queues_size[data_node]))) + '/' +
                       str(queues_size[data_node]) + ' paths from "' +
                       data_node + '"'
                       for data_node in data_node_list
                       if queues_size[data_node] > 0]

    if (hasattr(options, 'silent') and not options.silent and
       len(string_to_print) > 0):
        print('Remaining retrieval from data nodes: \n' +
              ' | '.join(string_to_print) + '\nProgress: ')

    if hasattr(options, 'serial') and options.serial:
        remote_netcdf_kwargs, time_var = setup_download_processes(options)
        for data_node in data_node_list:
            q_manager.queues.put(data_node, 'STOP')
            worker_retrieve(q_manager, data_node, time_var,
                            remote_netcdf_kwargs)

    renewal_time, failed = worker_exit(q_manager, data_node_list,
                                       queues_size, start_time,
                                       renewal_time, output,
                                       options)

    if failed:
        raise Exception('Retrieval failed')

    if (hasattr(options, 'silent') and
        not options.silent and
       len(string_to_print) > 0):
        print('\nDone!')
    return output


def progress_report(file_type, result, q_manager, data_node_list,
                    queues_size, start_time, renewal_time, failed,
                    output, options):
    elapsed_time = datetime.datetime.now() - start_time
    # renewal_elapsed_time = datetime.datetime.now() - renewal_time

    if file_type == 'HTTPServer':
        if result != 'FAIL':
            if hasattr(options, 'silent') and not options.silent:
                if result is not None:
                    print('\t' + result + '\n' + str(elapsed_time))
        else:
            failed = True
    else:
        if result != 'FAIL':
            assign_tree(output, *result)
            output.sync()
            string_to_print = [str(queues_size[data_node] -
                                   q_manager.queues.qsize(data_node))
                               .zfill(len(str(queues_size[data_node]))) +
                               '/' + str(queues_size[data_node])
                               for data_node in data_node_list
                               if queues_size[data_node] > 0]
            if (hasattr(options, 'silent') and not options.silent and
               len(string_to_print) > 0):
                print(str(elapsed_time) + ', ' + ' | '.join(string_to_print) +
                      '\r'),
        else:
            failed = True

    return renewal_time, failed


def assign_tree(output, val, sort_table, tree):
    if len(tree) > 1:
        if tree[0] != '':
            assign_tree(output.groups[tree[0]], val, sort_table, tree[1:])
        else:
            assign_tree(output, val, sort_table, tree[1:])
    else:
        output.variables[tree[0]][sort_table, ...] = val
    return
