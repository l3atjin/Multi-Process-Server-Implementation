"""See unit test function docstring."""

import filecmp
import multiprocessing
import pytest
import mapreduce
import utils
from utils import TESTDATA_DIR


@pytest.fixture(name="processes")
def processes_setup_teardown():
    """Store list of Master and Worker processes, then clean up after test.

    The code before the "yield" statement is setup code, which is executed
    before the test.  Code after the "yield" is teardown code, which is
    executed at the end of the test.  Teardown code is executed whether the
    test passed or failed.
    """
    # Setup code: store a list of processes.  Later, the testcase will append
    # to this list.
    processes = []

    # Transfer control to testcase
    yield processes

    # Teardown code: force all processes to terminate
    for process in processes:
        process.terminate()
        process.join()


def test_integration_01_grep(processes):
    """Run a grep MapReduce job."""
    utils.create_and_clean_testdir("test_integration_01")

    # Start Master
    process = multiprocessing.Process(
        name="Master:7000",
        target=mapreduce.Master,
        args=(7000,)
    )
    process.start()
    processes.append(process)
    utils.wait_for_process_is_ready(process, port=7000)

    # Start workers
    for worker_port in [7001, 7002, 7003]:
        process = multiprocessing.Process(
            name="Worker:{}".format(worker_port),
            target=mapreduce.Worker,
            args=(7000, worker_port),
        )
        process.start()
        processes.append(process)
        utils.wait_for_process_is_ready(process, port=worker_port)

    # Send new master job message
    utils.send_message({
        "message_type": "new_master_job",
        "input_directory": TESTDATA_DIR/"input/",
        "output_directory": "tmp/test_integration_01/output/",
        "mapper_executable": TESTDATA_DIR/"exec/grep_map.py",
        "reducer_executable": TESTDATA_DIR/"exec/grep_reduce.py",
        "num_mappers": 2,
        "num_reducers": 1
    }, port=7000)

    # Wait for master to create output
    utils.wait_for_isfile("tmp/test_integration_01/output/outputfile01")

    # Verify final output file contents
    assert filecmp.cmp(
        TESTDATA_DIR/"correct/grep_correct.txt",
        "tmp/test_integration_01/output/outputfile01",
        shallow=False
    )

    # Send shutdown message
    utils.send_message({
        "message_type": "shutdown"
    }, port=7000)

    # Wait for processes to stop
    utils.wait_for_process_all_stopped(processes)

    # Check for clean exit
    for process in processes:
        assert process.exitcode == 0, \
            "{} exitcode={}".format(process.name, process.exitcode)
