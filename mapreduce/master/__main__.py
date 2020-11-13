import os
import logging
import json
import threading
import time
import click
import socket
import mapreduce.utils
import pathlib
import datetime


# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Master:
    # Member Variables
    numJobs = 0
    job_queue = []
    worker_dict = dict()
    thread_pulse = threading.Thread()
    thread_hb = threading.Thread()
    isShutdown = False

    # Initialization / Constructor
    def __init__(self, port):
        """ Initialize the Master Object."""
        # Initialize Member Vars
        self.thread_pulse  = threading.Thread(target=self.check_pulse, args=[])
        
        # Create tmp folder for output
        cwd = os.getcwd() + "/tmp"
        dir = pathlib.Path(cwd)
        dir.mkdir(parents=True, exist_ok=True)

        # Creat Heartbeat Thread
        self.thread_hb = threading.Thread(target=self.listen_hb, args=[port])
        self.thread_pulse.start()
        self.thread_hb.start()

        # Create additional threads  (fault tolerance)
            # TODO still 

        # Create Message listening Socket
        thread = threading.Thread(target=self.listen, args=[port])
        thread.start() # This gives up execution to the 'listen' thread
        thread.join() # Wait for listen thread to shut down
        print("After listen join")
        self.thread_hb.join()
        print("in between two joins")
        self.thread_pulse.join()
        print("ACTUALLY ENDED")


    # Helper Functions
    def send_message(self, port, context):
        """Send TCP message to a given port."""
        sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sendSock.connect(("localhost", port))
        message = json.dumps(context)
        sendSock.sendall(message.encode('utf-8'))
        sendSock.close()

    def check_pulse(self):
        """Updates workers' statuses by last recieved heartbeat."""
        while True:
            if self.isShutdown:
                print("check_pulse shuts down")
                return
            for worker in self.worker_dict:
                if self.worker_dict[worker]["status"] != "dead":
                    diff = datetime.datetime.now() - self.worker_dict[worker]["last_ping"]
                    if diff.total_seconds() > 10:
                        self.worker_dict[worker]["status"] = "dead"
            time.sleep(1)

    # Big Functions
    def listen_hb(self, port):
        UDP_IP = "localhost"
        UDP_PORT = port-1
        sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
        sock.bind((UDP_IP, UDP_PORT))
        sock.settimeout(1)
        
        if self.isShutdown:
            sock.close()
            logging.info("listen_hb shuts down")
            return

        while True:
            print(self.isShutdown)

            if self.isShutdown:
                sock.close()
                logging.info("listen_hb shuts down")
                return
            # this could be a different thread
            
            # Thread for checking pulse, thread for receiving heartbeats
            
            # Create structure that has pid as key and pings since last heartbeat as value 
            # Listen for heartbeat and populate a 
            # Dictionary of hearbeat messages every 2 seconds
            # For each worker's message, update value to 0, all others value++
            # If value for any worker  > 5, mark dead and send shutdown message to it
            # Figure out how the big while loop works. Does it get iterated constantly or 
            # only when it receives a message?
            # Built-in message queue?\
            # https://piazza.com/class/k9vihaw2wd07b0?cid=2635
            data = str()
            while True:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    break
                if(len(data) < 4096):
                    break

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_str = data.decode("utf-8")
            message_dict = json.loads(message_str)
        
            # Check to make sure it is a heartbeat
            if(message_dict["message_type"] != "heartbeat"):
                continue
            else:   
                # update the corresponding worker's last received ping
                self.worker_dict[message_dict["worker_pid"]]["last_ping"] = datetime.datetime.now()

        if self.isShutdown:
            sock.close()
            logging.info("listen_hb shuts down")
            return

        sock.close()
        logging.info("end of listen hb")
        return
            
    def listen(self, port):
        """Listen for incoming messages."""

        # Set up Socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))
        sock.listen()

        sock.settimeout(1)

        while True:
            # Listen for a connection for 1s.  The socket library avoids consuming CPU
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Get message chunk by chunk 
            message_chunks = []
            while True:
                try:
                    data = clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)
            clientsocket.close()

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            
            # If Received Message
            if message_dict['message_type']:
                
                # Shutdown Message Received
                if message_dict["message_type"] == "shutdown":
                    self.isShutdown = True
                    if self.isShutdown:
                        print("Shutdown should be true")
                    context = {
                        "message_type": "shutdown"
                    }
                    # Loop through Workers
                    for worker in self.worker_dict:
                        # Skip if dead
                        if self.worker_dict[worker]["status"] == "dead":
                            continue
                        print("Worker:")
                        print(worker)
                        print(self.worker_dict[worker])
                        # Else Send Shutdown Message
                        sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSock.connect(("localhost", self.worker_dict[worker]["worker_port"]))
                        message = json.dumps(context)
                        sendSock.sendall(message.encode('utf-8'))
                        sendSock.close()

                    print("Shutting Down.")
                    sock.close()
                    clientsocket.close()
                    print("closed socket")
                    print("joined thread")
                    return
                
                # New Job Received
                elif message_dict["message_type"] == "new_master_job":
                    # Master Job Shit
                    print("Received:")
                    print(message_dict)

                    # Grab values from job message 

                    # Create new set of direcories for temp job files
                    cwd = os.getcwd() + "/tmp/job-{numJobs}/"
                    
                    # Mapper output dir
                    dir = pathlib.Path(cwd + "mapper-output/")
                    dir.mkdir(parents=True, exist_ok=True)

                    # Grouper output dir
                    dir = pathlib.Path(cwd + "grouper-output/")
                    dir.mkdir(parents=True, exist_ok=True)

                    # Reducer output dir
                    dir = pathlib.Path(cwd + "reducer-output/")
                    dir.mkdir(parents=True, exist_ok=True)

                    # Check if workers are ready to work (get num available workers)
                    available_workers = 0
                    busy_workers = 0
                    for worker in worker_dict:
                        if worker_dict[worker]["status"] == "ready":
                            available_workers += 1
                        elif worker_dict[worker]["status"] == "busy":
                            busy_workers += 1
                            continue

                    # ---------------- TODO: ----------------------------
                    # Check if MapReduce server is currently working on job

                    # OH Questions:
                        # how do u check if mapreduce server is running a job is it a bool?? check if workers are busy
                        # utils.py where does it come in?
                        # check job queue at end of each job? 
                        # do shutdown actually shutdown the mapreduce server
                        # mapreduce script
                        # integration test
                        # use subprocess to run code from another code
                    if busy_workers > 0 or available_workers == 0:
                        self.job_queue.push(message_dict)
                        continue
                    else:
                        self.do_job(message_dict)
                    # ---------------------------------------------------

                # Registration Message Received
                elif message_dict["message_type"] == "register":
                    print("Received:")
                    print(message_dict)
                    worker_port =  message_dict["worker_port"]
                    worker_pid =  message_dict["worker_pid"]

                    # Create registration ack message
                    context = {
                        "message_type": "register_ack",
                        "worker_host": 'localhost',
                        "worker_port": worker_port,
                        "worker_pid" : worker_pid
                    }
                    # Open Socket, Send to Worker 
                    sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSock.connect(("localhost", worker_port))
                    message = json.dumps(context)
                    sendSock.sendall(message.encode('utf-8'))
                    sendSock.close()
                    print("Sent:")
                    print(message)
                    
                    # Add worker to dictionary
                    self.worker_dict[worker_pid] = {"worker_port": worker_port, "status": "ready", "last_ping": datetime.datetime.now()}
                    print("Worker dict: ")
                    print(self.worker_dict)
                    continue

                # Status Message Received
                elif message_dict["message_type"] == "status":
                    print("Received:")
                    print(message_dict)
                    worker_pid =  message_dict["worker_pid"]
                    # TODO: Find out what else to do when update status
                    # Update status
                    if message_dict["status"] == "finished":
                        self.worker_dict[worker_pid]["status"] = "ready"
                    continue

            else:
                print("ERROR. INVALID REQUEST")
                continue

        print("listen() shutting down")

    def do_job(self, input_directory, output_directory, mapper_executable,
                reducer_executable, num_mappers, num_reducers):
        """Execute the different phases of a MapReduce Job."""
        
        input_directory =  message_dict["input_directory"]
        mapper_executable =  message_dict["mapper_executable"]
        reducer_executable =  message_dict["reducer_executable"]
        num_mappers =  message_dict["num_mappers"]
        num_reducers =  message_dict["num_reducers"]


        ### Mapper Phase ###

        # Get files from input_dir, sort alphabetically
        map_output_path = os.getcwd() + "/tmp/job-{numJobs}/mapper-output/"
        inPath = os.getcwd() + "/tmp/job-{numJobs}/input_directory/"
        infile_list = os.listdir(inPath)
        sort(infile_list)

        # Make empty list of lists
        count = 0
        mapper_tasks = []
        for i in range(num_mappers):
            mapper_tasks.append([])

        # Round Robin fill list
        while count < len(infile_list):
            for mapper in mapper_tasks:
                mapper.append(infile_list[count])
                count += 1
                if count == num_mappers:
                    break
        print(str(mapper_tasks))

        # Assign each list of tasks to a worker
        list(self.worker_dict) # Sorts workers by registration order
        assigned = False
        for tasks in mapper_tasks:
            assigned = False
            # Try each worker (IN REGISTRATION ORDER) untill assigned
            while not assigned:
                for worker in self.worker_dict:
                    # Send message if worker is ready 
                    if self.worker_dict[worker]["status"] == "ready":
                        # Create Message
                        context = {
                            "message_type": "new_worker_job",
                            "input_files": tasks,
                            "executable": mapper_executable,
                            "output_directory": map_output_path,
                            "worker_pid" : self.worker_dict[worker]["pid"]
                        }
                        self.worker_dict[worker]["status"] = "busy"
                        # Open Socket and Send
                        send_message(self.worker_dict[worker]["worker_port"], context)
                        assigned = True 
                        break # from this task assignment, onto next
            # End While
        # End For
        


        # Get files from grouper_dir, sort alphabetically
        reducer_output_path = os.getcwd() + "/tmp/job-{numJobs}/reducer-output/"
        inPath = os.getcwd() + "/tmp/job-{numJobs}/grouper-output/"
        infile_list = os.listdir(inPath)
        sort(infile_list)

        # Make empty list of lists
        count = 0
        reducer_tasks = []
        for i in range(num_reducers):
            reducer_tasks.append([])

        # Round Robin fill list
        while count < len(infile_list):
            for reducer in reducer_tasks:
                reducer.append(infile_list[count])
                count += 1
                if count == num_reducers:
                    break
        print(str(reducer_tasks))

        # Assign each list of tasks to a worker
        list(self.worker_dict) # Sorts workers by registration order
        assigned = False
        for tasks in reducer_tasks:
            assigned = False
            # Try each worker (IN REGISTRATION ORDER) untill assigned
            while not assigned:
                for worker in self.worker_dict:
                    # Send message if worker is ready
                    if self.worker_dict[worker]["status"] == "ready":
                        # Create Message
                        context = {
                            "message_type": "new_worker_job",
                            "input_files": tasks,
                            "executable": reducer_executable,
                            "output_directory": reducer_output_path,
                            "worker_pid" : self.worker_dict[worker]["pid"]
                        }
                        self.worker_dict[worker]["status"] = "busy"
                        # Open Socket and Send
                        send_message(self.worker_dict[worker]["worker_port"], context)
                        assigned = True 
                        break # from this task assignment, onto next
            # End While
        # End For
        # endReducer

        # Increment numJobs when finished 
        numJobs += 1


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)
    # Learn to kill yourself and the workers
    # possibly the answer is process.terminate

if __name__ == '__main__':
    main()

