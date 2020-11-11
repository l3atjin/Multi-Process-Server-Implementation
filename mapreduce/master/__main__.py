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
        thread_hb = threading.Thread(target=self.listen_hb, args=[port])
        thread_hb.start()

        # Create additional threads  (fault tolerance)
            # TODO still 

        # Create Message listening Socket
        thread = threading.Thread(target=self.listen, args=[port])
        thread.start() # This gives up execution to the 'listen' thread
        thread.join() # Wait for listen thread to shut down
        thread_hb.join()
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
                return
            for worker in self.worker_dict:
                if self.worker_dict[worker]["status"] != "dead":
                    diff = datetime.datetime.now() - self.worker_dict[worker]["last_ping"]
                    if diff.total_seconds() > 10:
                        self.worker_dict[worker]["status"] = "dead"
            time.sleep(1)

    # Big Functions
    def listen_hb(self, port):
        UDP_IP = "127.0.0.1"
        UDP_PORT = port-1
        sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
        sock.bind((UDP_IP, UDP_PORT))
        sock.settimeout(1)
        self.thread_pulse.start()

        while True:
            if self.isShutdown:
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
            while True:
                try:
                    data = sock.recv(4096)
                    break
                except socket.timeout:
                    continue
            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_str = data.decode("utf-8")
            message_dict = json.loads(message_str)
        
            # Check to make sure it is a heartbeat
            if(message_dict["message_type"] != "heartbeat"):
                continue
            else:   
                # update the corresponding worker's last received ping
                self.worker_dict[message_dict["worker_pid"]]["last_ping"] = datetime.datetime.now()
            

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
                    self.isShutdown = True
                    sock.close()
                    clientsocket.close()
                    print("closed socket")
                    self.thread_pulse.join()
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

                    numJobs += 1
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

            else:
                print("ERROR. INVALID REQUEST")
                continue

        print("listen() shutting down")

    def do_job(self, input_directory, output_directory, mapper_executable,
                reducer_executable, num_mappers, num_reducers):
        """Execute the different phases of a MapReduce Job."""
        
        input_directory =  message_dict["input_directory"]
        output_directory =  message_dict["output_directory"]
        mapper_executable =  message_dict["mapper_executable"]
        reducer_executable =  message_dict["reducer_executable"]
        num_mappers =  message_dict["num_mappers"]
        num_reducers =  message_dict["num_reducers"]

        # ---------- TODO: Input partioning ------------
        # Get files from input_dir,
        # Sort files alphabetically and allocate into num_mappers groups
        # "Round robin". 
        # # num_mappers is how many groups  of files
        # ----------------------------------------------

        # For each worker (IN REGISTRATION ORDER -- list should do this)
        list(self.worker_dict)
        for worker in self.worker_dict:
            # Create message with relevant info
            context = {
            sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sendSock.connect(("localhost", self.worker_dict[worker]["worker_port"]))
            message = json.dumps(context)

            # Send job message to worker 
            sendSock.sendall(message.encode('utf-8'))
            sendSock.close()
            print("Sent:")
            print(message)

@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)
    # Learn to kill yourself and the workers
    # possibly the answer is process.terminate


if __name__ == '__main__':
    main()

