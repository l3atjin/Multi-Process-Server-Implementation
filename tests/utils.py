"""Unit test utilities used by many tests."""
import os
import subprocess
import pathlib
import shutil
import json
import time
import threading
import socket


# Temporary directory.  Tests will create files here.
TMPDIR = pathlib.Path("tmp")

# Directory containing unit test input files, mapper executables,
# reducer executables, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"

# Time in between two heart beats.  This in the spec.
TIME_BETWEEN_HEARTBEATS = 2

# Default timeout
TIMEOUT = 10

# Timeout for tests where we need to wait for workers to die
TIMEOUT_LONG = 3 * TIMEOUT


class PathJSONEncoder(json.JSONEncoder):
    """
    Extended the Python JSON encoder to encode Pathlib objects.

    Docs: https://docs.python.org/3/library/json.html

    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """

    # Avoid pylint false positive.  There's a style problem in the JSON library
    # that we're inheriting from.
    # https://github.com/PyCQA/pylint/issues/414#issuecomment-212158760
    # pylint: disable=E0202

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)


def create_and_clean_testdir(basename):
    """Remove and recreate {TMPDIR}/basename then remove {TMPDIR}/job-*."""
    # Clean up {TMPDIR}/job-*/
    for jobdir in TMPDIR.glob("job-*"):
        shutil.rmtree(jobdir)

    # Clean up {TMDIR}/basename/
    dirpath = TMPDIR/basename
    if dirpath.exists():
        shutil.rmtree(dirpath)
    dirpath.mkdir(parents=True)


def worker_heartbeat_generator(*worker_pids):
    """Fake Worker heartbeat messages."""
    while True:
        time.sleep(TIME_BETWEEN_HEARTBEATS)
        # Avoid sending heartbeats too fast
        for pid in worker_pids:
            time.sleep(TIME_BETWEEN_HEARTBEATS)
            # Avoid sending heartbeats too fast
            yield json.dumps({
                "message_type": "heartbeat",
                "worker_pid": pid
            }).encode('utf-8')


def get_messages(mock_socket):
    """Return a list decoded JSON messages sent via mock_socket sendall()."""
    messages = []
    for args, _ in mock_socket.return_value.sendall.call_args_list:
        message_str = args[0].decode('utf-8')
        message_dict = json.loads(message_str)
        messages.append(message_dict)
    return messages


def is_map_message(message):
    """Return True if message starts a map job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_worker_job" and

        "output_directory" in message and
        "mapper-output" in message["output_directory"]
    )


def is_sort_message(message):
    """Return True if message is a sort job message."""
    return (
        "message_type" in message and
        message["message_type"] == "new_sort_job"
    )


def is_reduce_message(message):
    """Return True if message starts a reduce job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_worker_job" and

        "output_directory" in message and
        "reducer-output" in message["output_directory"]
    )


def is_status_finished_message(message):
    """Return True message is a status finished message."""
    return (
        "message_type" in message and
        message["message_type"] == "status" and
        "status" in message and
        message["status"] == "finished"
    )


def is_heartbeat_message(message):
    """Return True if message is a heartbeat message."""
    return (
        "message_type" in message and
        message["message_type"] == "heartbeat"
    )


def filter_heartbeat_messages(messages):
    """Return a subset of messages including only heartbeat messages."""
    return [m for m in messages if is_heartbeat_message(m)]


def filter_not_heartbeat_messages(messages):
    """Return a subset of messages excluding heartbeat messages."""
    return [m for m in messages if not is_heartbeat_message(m)]


def wait_for_isdir(*args):
    """Verify master created the correct job directory structure."""
    for _ in range(TIMEOUT):
        if all(os.path.isdir(dirname) for dirname in args):
            return
        time.sleep(1)
    raise Exception("Failed to create directories {}".format(args))


def wait_for_isfile(*args):
    """Verify master created the correct job directory structure."""
    for _ in range(TIMEOUT):
        if all(os.path.exists(filename) for filename in args):
            return
        time.sleep(1)
    raise Exception("Failed to create files {}".format(args))


def wait_for_messages(function, mock_socket, num=1):
    """Return when function() evaluates to True on num messages."""
    for _ in range(TIMEOUT_LONG):
        messages = get_messages(mock_socket)
        n_true_messages = sum(function(m) for m in messages)
        if n_true_messages == num:
            return
        time.sleep(1)
    raise Exception("Expected {} messages, got {}.".format(
        num, n_true_messages
    ))


def wait_for_status_finished_messages(mock_socket, num=1):
    """Return after num status finished messages."""
    return wait_for_messages(is_status_finished_message, mock_socket, num)


def wait_for_map_messages(mock_socket, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_map_message, mock_socket, num)


def wait_for_reduce_messages(mock_socket, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_reduce_message, mock_socket, num)


def wait_for_sort_messages(mock_socket, num=1):
    """Return after num sort messages."""
    return wait_for_messages(is_sort_message, mock_socket, num)


def wait_for_threads(num=1):
    """Return after the total number of threads is num."""
    for _ in range(TIMEOUT):
        if len(threading.enumerate()) == num:
            return
        time.sleep(1)
    raise Exception("Failed to close threads.")


def send_message(message, port):
    """Send JSON-encoded TCP message."""
    host = "localhost"
    message_str = json.dumps(message, cls=PathJSONEncoder)
    message_bytes = str.encode(message_str)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(message_bytes)
    sock.close()


def port_is_open(host, port):
    """Return True if host:port is open."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except ConnectionRefusedError:
        return False
    else:
        return True
    finally:
        sock.close()


def wait_for_process_is_ready(process, port):
    """Return when process is listening on port."""
    for _ in range(TIMEOUT):
        assert process.exitcode is None, \
            "{} unexpected exit with exitcode={}".format(
                process.name,
                process.exitcode,
            )
        if port_is_open("localhost", port):
            return
        time.sleep(1)
    raise Exception(
        "Process {} PID {} failed to start listening on port {}.".format(
            process.name, process.pid, port
        ))


def wait_for_process_all_stopped(processes):
    """Return after all processes are not alive."""
    for _ in range(TIMEOUT):
        if all(not p.is_alive() for p in processes):
            return
        time.sleep(1)
    raise Exception("Some processes failed to stop.")


def assert_no_prohibited_terms(*terms):
    """Check for prohibited terms before testing style."""
    for term in terms:
        completed_process = subprocess.run(
            [
                "grep",
                "-r",
                "-n",
                term,
                "--include=*.py",
                "--exclude=submit.py",
                "mapreduce"
            ],
            check=False,  # We'll check the return code manually
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

        # Grep exit code should be non-zero, indicating that the prohibited
        # term was not found.  If the exit code is zero, crash and print a
        # helpful error message with a filename and line number.
        assert completed_process.returncode != 0, (
            "The term '{term}' is prohibited.\n{message}"
            .format(term=term, message=completed_process.stdout)
        )
