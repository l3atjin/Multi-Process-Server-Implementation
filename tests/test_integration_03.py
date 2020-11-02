"""See unit test function docstring."""

import os
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


def test_integration_03_many_mappers(processes):
    """Run a word count MapReduce job with more mappers and reducers."""
    utils.create_and_clean_testdir("test_integration_03")

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
        "output_directory": "tmp/test_integration_03/output/",
        "mapper_executable": TESTDATA_DIR/"exec/wc_map.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 4,
        "num_reducers": 2
    }, port=7000)

    # Wait for master to create output
    utils.wait_for_isfile(
        "tmp/test_integration_03/output/outputfile01",
        "tmp/test_integration_03/output/outputfile02",
    )

    # Verify number of files
    assert len(os.listdir("tmp/test_integration_03/output/")) == 2

    # Verify final output file contents
    with Path("tmp/test_integration_03/output/outputfile01").open() as infile:
        outputfile1 = infile.readlines()
    with Path("tmp/test_integration_03/output/outputfile02").open() as infile:
        outputfile2 = infile.readlines()
    actual = sorted(outputfile1 + outputfile2)
    with Path(TESTDATA_DIR/"correct/word_count_correct.txt").open() as infile:
        correct = sorted(infile.readlines())
    assert actual == correct
