"""See unit test function docstring."""

import os
import json
import socket
import time
import mapreduce
import utils


def master_message_generator():
    """Fake Master messages."""
    # First message
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": os.getpid(),
    }).encode('utf-8')
    yield None

    # Wait long enough for Worker to send two heartbeats
    time.sleep(1.5 * utils.TIME_BETWEEN_HEARTBEATS)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_worker_02_heartbeat(mocker):
    """Verify worker sends heartbeat messages to the master.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    # Mock socket library functions to return sequence of hardcoded values
    mockclientsocket = mocker.Mock()
    mockclientsocket.recv.side_effect = master_message_generator()

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket = mocker.patch('socket.socket')
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

    # Verify messages sent by the Worker, excluding heartbeat messages
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3001,
            "worker_pid": os.getpid(),
        },
    ]

    # Verify UDP socket configuration.  This is the socket the worker uses to
    # send heartbeat messages to the master.
    mock_socket.assert_has_calls([
        mocker.call(socket.AF_INET, socket.SOCK_DGRAM),
        mocker.call().connect(('localhost', 2999)),
    ], any_order=True)

    # Verify heartbeat messages sent by the Worker
    heartbeat_messages = utils.filter_heartbeat_messages(all_messages)
    assert heartbeat_messages == [
        {
            "message_type": "heartbeat",
            "worker_pid": os.getpid(),
        },
        {
            "message_type": "heartbeat",
            "worker_pid": os.getpid(),
        },
    ]
