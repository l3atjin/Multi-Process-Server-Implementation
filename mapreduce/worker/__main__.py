import os
import sys
import pathlib
import subprocess
import logging
import threading
import socket
import json
import time
import click
import mapreduce.utils
import heapq


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    isShutdown = False
    isRegistered = False
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd()) 

        # Get workers Process ID
        pid = os.getpid()
        # Create TCP Socket on worker_port, call listen()
        thread = threading.Thread(target=self.listen, args=[master_port, worker_port])
        thread.start() # This gives up execution to the 'listen' thread
        # call functions
        thread.join() # Wait for listen thread to shut down
        # thread_hb.join()
        # Send register to master
        # When receive register_ack, create new thread
            # Sends heartbeat messages to master
        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!

    # send context to port helper function
    def send_message(self, port, context):
        sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # make it a helper func
        sendSock.connect(("localhost", port))
        message = json.dumps(context)
        sendSock.sendall(message.encode('utf-8'))
        sendSock.close()

    # send heartbeat
    def send_hb(self,master_port, worker_port):
        # Send hb Message to Master
        context = {
            "message_type": "heartbeat",
            "worker_pid": os.getpid()
        }
        while True:
            if self.isShutdown:
                return
            message = json.dumps(context)
            # Here it is
            UDP_IP = "localhost"
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
            udp_sock.connect((UDP_IP, master_port-1))
            udp_sock.sendall(message.encode('utf-8'))
            udp_sock.close()
            time.sleep(2)


    def listen(self,master_port, worker_port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", worker_port))
        sock.listen()
        thread_hb = threading.Thread(target=self.send_hb, args=[master_port, worker_port])

        # Send Register Message to Master
        context = {
            "message_type" : "register",
            "worker_host" : "localhost",
            "worker_port" : worker_port,
            "worker_pid" : os.getpid()
        }
        self.send_message(master_port, context)
        print("just sent register")
        
        # receive register_ack, then create thread f or heartbeat sending

        # Connect to a client
        sock.settimeout(1)

        while True:
            # Listen for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Receive data, one chunk at a time.  If recv() times out before we can
            # read a chunk, then go back to the top of the loop and try again.
            # When the client closes the connection, recv() returns empty data,
            # which breaks out of the loop.  We make a simplifying assumption that
            # the client will always cleanly close the connection.
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
            print(message_dict)
            

            # If Received Message
            if message_dict['message_type']:
                # Check registration, check for register ack
                if not self.isRegistered: 
                    if message_dict["message_type"] == "register_ack":
                        self.isRegistered = True
                        thread_hb.start()
            
                # If Shutdown Message
                elif message_dict["message_type"] == "shutdown":
                    self.isShutdown = True
                    print("Worker shutting Down.")
                    clientsocket.close()
                    thread_hb.join()
                    return

                elif message_dict["message_type"] == "new_worker_job":
                    # Master  Job Shit
                    input_files =  message_dict["input_files"]
                    output_dir =  pathlib.Path(message_dict["output_directory"])
                    executable =  pathlib.Path(message_dict["executable"])
                    for name in input_files:
                        with open(output_dir/name, 'w') as f:
                            p = subprocess.run(executable, stdout=f, stdin=name)
                    
                    context = {
                        "message_type": "status",
                        "output_files" : input_files,
                        "status": "finished",
                        "worker_pid": os.getpid()
                    }
                    self.send_message(master_port, context)
                    continue

                # If Sorting/Grouping Message
                elif message_dict["message_type"] == "new_sort_job":
                    # Grab data from job message
                    input_files = message_dict["input_files"]
                    output_file = message_dict["output_file"]
                    
                    # Sort each Input File line by line
                    for infile in input_files:
                        lines=file(infile).readlines()
                        lines.sort()
                        # write sorted lines into the file
                    
                    # Combine Sorted Files into one Output File
                    for infile in input_files:
                        print("merging")
                        # do some (heapq?) merge to make output_file

                    # Send message to Master once completed.
                    context = {
                                "message_type": "status",
                                "output_file" : output_file,
                                "status": "finished",
                                "worker_pid": os.getpid()
                            }
                    self.send_message(master_port,context)
                    continue
            else:
                print("ERROR. INVALID REQUEST")
                continue 

        print("listen() shutting down")

@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)

if __name__ == '__main__':
    main()
