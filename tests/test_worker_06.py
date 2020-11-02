"""See unit test function docstring."""

import os
import shutil
import json
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def master_message_generator(mock_socket):
    """Fake Master messages."""
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": os.getpid(),
    }).encode('utf-8')
    yield None

    # Send new map job
    yield json.dumps({
        'message_type': 'new_sort_job',
        'input_files': [
            'tmp/test_worker_06/group_input01',
            'tmp/test_worker_06/group_input02',
        ],
        'output_file': "tmp/test_worker_06/merged",
        'worker_pid': os.getpid(),
    }).encode('utf-8')
    yield None

    # Wait for worker to finish map job
    utils.wait_for_status_finished_messages(mock_socket)

    # Shut down
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_worker_06_sort_two_inputs(mocker):
    """Verify worker correctly completes a sort job with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_worker_06")
    shutil.copyfile(
        TESTDATA_DIR/'test_worker_06/test_worker_06.group_input01',
        "tmp/test_worker_06/group_input01",
    )
    shutil.copyfile(
        TESTDATA_DIR/'test_worker_06/test_worker_06.group_input02',
        "tmp/test_worker_06/group_input02",
    )

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.Mock()
    mockclientsocket.recv.side_effect = master_message_generator(mock_socket)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Run student worker code.  When student worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            3000,  # Master port
            3001,  # Worker port
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3001,
            "worker_pid": os.getpid(),
        },
        {
            "message_type": "status",
            "output_file": "tmp/test_worker_06/merged",
            "status": "finished",
            "worker_pid": os.getpid(),
        },
    ]

    # Verify group stage output
    with Path("tmp/test_worker_06/merged").open() as infile:
        actual = infile.readlines()
    assert actual == sorted([
        "\t1\n",
        "\t1\n",
        "bye\t1\n",
        "goodbye\t1\n",
        "hadoop\t1\n",
        "hadoop\t1\n",
        "hello\t1\n",
        "hello\t1\n",
        "world\t1\n",
        "world\t1\n",
    ])
