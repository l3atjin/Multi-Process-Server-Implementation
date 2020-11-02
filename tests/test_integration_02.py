"""See unit test function docstring."""

import multiprocessing
from pathlib import Path
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


def test_integration_02_wordcount(processes):
    """Run a word count MapReduce job."""
    utils.create_and_clean_testdir("test_integration_02")

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
    for worker_port in [7001, 7002]:
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
        "output_directory": "tmp/test_integration_02/output/",
        "mapper_executable": TESTDATA_DIR/"exec/wc_map.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 2,
        "num_reducers": 1
    }, port=7000)

    # Wait for master to create output
    utils.wait_for_isfile("tmp/test_integration_02/output/outputfile01")

    # Verify final output file contents
    with Path("tmp/test_integration_02/output/outputfile01").open() as infile:
        actual = sorted(infile.readlines())
    with Path(TESTDATA_DIR/"correct/word_count_correct.txt").open() as infile:
        correct = sorted(infile.readlines())
    assert actual == correct

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
