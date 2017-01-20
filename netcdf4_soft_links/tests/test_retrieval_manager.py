from netcdf4_soft_links import retrieval_manager, queues_manager
from argparse import Namespace


def test__get_time_var():
    options = Namespace(time_var='TIME')
    assert retrieval_manager._get_time_var(options) == 'TIME'


def test_start_dl_processes_no_serial():
    num_dl = 2
    options = Namespace(num_dl=num_dl)
    q_manager = queues_manager.NC4SL_queues_manager(options, ['MainProcess'])
    q_manager.queues.add_new_data_node('test')
    processes = (retrieval_manager
                 .start_download_processes_no_serial(q_manager, num_dl,
                                                     dict()))
    assert sorted(processes.keys()) == ['test-0', 'test-1']
    processes = retrieval_manager.stop_download_processes(processes)
    for process_name in processes:
        processes[process_name].join()
        assert not processes[process_name].is_alive()
