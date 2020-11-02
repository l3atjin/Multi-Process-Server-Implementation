"""A Simple Round Robin Grouping Test."""

import sys
import json
import os
import distutils.dir_util
import filecmp
import resource
import time
import threading
import mapreduce
import utils
from utils import TESTDATA_DIR


def worker_message_generator(mock_socket, memory_profiler):
    """Fake Worker messages."""
    # Workers register
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": 1001,
    }).encode('utf-8')
    yield None
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3002,
        "worker_pid": 1002,
    }).encode('utf-8')
    yield None

    # User submits new job with large input files
    yield json.dumps({
        'message_type': 'new_master_job',
        'input_directory': TESTDATA_DIR/'input_large',
        'output_directory': "tmp/test_master_08/output",
        'mapper_executable': TESTDATA_DIR/'exec/wc_map.sh',
        'reducer_executable': TESTDATA_DIR/'exec/wc_reduce.sh',
        'num_mappers': 2,
        'num_reducers': 2
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for Master to create directories
    utils.wait_for_isdir("tmp/job-0")

    # Simulate files created by Worker
    distutils.dir_util.copy_tree(
        TESTDATA_DIR/"test_master_08/intermediate/job-0",
        "tmp/job-0"
    )

    # Wait for Master to send 2 map messages because num_mappers=2
    utils.wait_for_map_messages(mock_socket, num=2)

    # Status finished message from both mappers
    yield json.dumps({
        "message_type": "status",
        "output_files": [
            "tmp/job-0/mapper-output/file01",
            "tmp/job-0/mapper-output/file03",
        ],
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None
    yield json.dumps({
        "message_type": "status",
        "output_files": [
            "tmp/job-0/mapper-output/file02",
            "tmp/job-0/mapper-output/file04",
        ],
        "status": "finished",
        "worker_pid": 1002
    }).encode('utf-8')
    yield None

    # Start tracking memory usage
    memory_profiler.start()

    # Wait for Master to send sort job messages
    utils.wait_for_sort_messages(mock_socket, num=2)

    # Sort job status finished
    yield json.dumps({
        "message_type": "status",
        "output_file": "tmp/job-0/grouper-output/sorted01",
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None
    yield json.dumps({
        "message_type": "status",
        "output_file": "tmp/job-0/grouper-output/sorted02",
        "status": "finished",
        "worker_pid": 1002
    }).encode('utf-8')
    yield None

    # Wait for Master to send reduce job message
    utils.wait_for_reduce_messages(mock_socket, num=2)

    # Verify group stage memory usage.  We need to check here because the only
    # way we know that grouping is completely done is when the Master sends a
    # message to begin the reduce phase.  The instructor solution group phase
    # requries less than 1 MB memory and less than 1 s on a modern machine.
    memory_profiler.stop()

    # Shutdown
    # Early Shutdown since TM8 is only testing proper groupings
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_master_08_roundrobin(mocker):
    """Verify content of map messages sent by the master.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_master_08")

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.Mock()
    memory_profiler = MemoryProfiler()
    mockclientsocket.recv.side_effect = worker_message_generator(
        mock_socket,
        memory_profiler,
    )

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.recv.side_effect = \
        utils.worker_heartbeat_generator(1001, 1002)

    # Run student master code.  When student master calls recv(), it will
    # return the faked responses configured above.
    try:
        mapreduce.master.Master(3000)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify group stage output files
    grouper_output_files = set(os.listdir("./tmp/job-0/grouper-output/"))
    assert grouper_output_files == set([
        'sorted01', 'sorted02', 'reduce01', 'reduce02',
    ])

    # Verify group stage round robin key partitioning
    assert filecmp.cmp(
        "./tmp/job-0/grouper-output/reduce01",
        TESTDATA_DIR/"test_master_08/correct/job-0/grouper-output/"
        "reduce01",
        shallow=False
    )
    assert filecmp.cmp(
        "./tmp/job-0/grouper-output/reduce02",
        TESTDATA_DIR/"test_master_08/correct/job-0/grouper-output/"
        "reduce02",
        shallow=False
    )

    # Verify time and memory constraints
    group_time_seconds = memory_profiler.get_time_delta()
    group_memory_bytes = memory_profiler.get_mem_delta()
    assert group_memory_bytes < 1 * 1024 * 1024  # 1 MB
    assert 0 < group_time_seconds < 10


class MemoryProfiler:
    """Monitory memory usage in a separate thread."""

    # Time between memory usage measurements in s
    PROFILE_INTERVAL = 0.05

    def __init__(self):
        """Start profiling."""
        self.run = True  # stop var
        self.mem_before = None
        self.mem_max = None
        self.time_start = None
        self.time_stop = None
        self.profile_thread = None

    def profile(self):
        """Measure memory usage periodically and store the max.

        This function runs in a separate thread.
        """
        self.mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.mem_max = self.mem_before
        while self.run:
            mem_cur = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            self.mem_max = max(mem_cur, self.mem_max)
            time.sleep(self.PROFILE_INTERVAL)

    def start(self):
        """Start profiler in a separate thread."""
        self.profile_thread = threading.Thread(target=self.profile)
        self.time_start = time.time()
        self.profile_thread.start()

    def stop(self):
        """Stop profiler."""
        self.time_stop = time.time()
        self.run = False
        self.profile_thread.join()

    def get_mem_delta(self):
        """Return max difference in memory usage (B) since start."""
        # macOS returns memory in B
        if sys.platform == "darwin":
            return self.mem_max - self.mem_before

        # Linux returns kB, convert to B
        if sys.platform == "linux":
            return (self.mem_max - self.mem_before) * 1024

        # Should never get here
        raise Exception("Unsupported platform {}".format(sys.platform))

    def get_time_delta(self):
        """Return time difference in seconds from start to stop."""
        return self.time_stop - self.time_start
