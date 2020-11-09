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
    # Member Variable
    worker_dict = dict()
    def __init__(self, port):
        # Create tmp folder
        # use pathlib with Parents = True and Exist_ok = True
        cwd = os.getcwd() + "/tmp"
        dir = pathlib.Path(cwd)
        dir.mkdir(parents=True, exist_ok=True)
        
        # Create new thread - listens for heartbeats on port-1
        # Have a dictionary of workers each having a status
        # in that thread make a socket on port-1
        # for loop that checkes for messages from each workers periodically
            # if after a certain period you dont receive any signal, mark that worker dead
            # upadte the status

        # Create additional threads  (fault tolerance)
        thread_hb = threading.Thread(target=self.listen_hb, args=[port])
        thread_hb.start()
        # Create new socket on port number, call listen() 
        thread = threading.Thread(target=self.listen, args=[port])
        thread.start() # This gives up execution to the 'listen' thread
        thread.join() # Wait for listen thread to shut down
        thread_hb.join()
        print("main() shutting down")

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")

    # helper func for sending messages
    def send_message(self, port, context):
        sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # make it a helper func
        sendSock.connect(("localhost", port))
        message = json.dumps(context)
        sendSock.sendall(message.encode('utf-8'))
        sendSock.close()

    # updates the worker status based on their last received ping
    def check_pulse(self):
        for worker in self.worker_dict:
            if self.worker_dict[worker]["status"] != "dead":
                diff = datetime.datetime.now() - self.worker_dict[worker]["last_ping"]
                if diff.total_seconds() > 10:
                    self.worker_dict[worker]["status"] = "dead"


    # listen for heartbeat
    def listen_hb(self, port):
        UDP_IP = "127.0.0.1"
        UDP_PORT = port-1
        sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
        sock.bind((UDP_IP, UDP_PORT))

        # Connect to a client
        sock.settimeout(1)

        while True:

            # this could be a different thread
            self.check_pulse()
            # Listen for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
            
            # Create structure that has pid as key and pings since last heartbeat as value 
            # Listen for heartbeat and populate a 
            # Dictionary of hearbeat messages every 2 seconds
            # For each worker's message, update value to 0, all others value++
            # If value for any worker  > 5, mark dead and send shutdown message to it
            # Figure out how the big while loop works. Does it get iterated constantly or 
            # only when it receives a message?
            # Built-in message queue?
            message_chunks = []
            while True:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)

            # Check to make sure it is a heartbeat
            if(message_dict["message_type"] != "heartbeat"):
                continue
            else:
                # update the corresponding worker's last received ping
                self.worker_dict[message_dict["worker_pid"]]["last_ping"] = datetime.datetime.now()
            print(message_dict)
            

    def listen(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))
        sock.listen()

        # Wait for incoming messages
            # Ignore invalid messages, try: block 
        # Return once all threads are exited 

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
            # what does this line do?
            clientsocket.close()

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            # print(message_dict)
            
            # If Received Message
            if message_dict['message_type']:
                # If Shutdown Message
                if message_dict["message_type"] == "shutdown":
                    # shutdown()
                    # kill each worker then self                 
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
                    return

                elif message_dict["message_type"] == "new_master_job":
                    # Master Job Shit
                    continue

                elif message_dict["message_type"] == "register":
                    print("Received:")
                    print(message_dict)
                    worker_port =  message_dict["worker_port"]
                    worker_pid =  message_dict["worker_pid"]
                    context = {
                        "message_type": "register_ack",
                        "worker_host": 'localhost',
                        "worker_port": worker_port,
                        "worker_pid" : worker_pid
                    }
                    # Send to worker
                    sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSock.connect(("localhost", worker_port))
                    message = json.dumps(context)
                    sendSock.sendall(message.encode('utf-8'))
                    sendSock.close()
                    print("Sent:")
                    print(message)
                    
                    # Add worker to container
                    self.worker_dict[worker_pid] = {"worker_port": worker_port, "status": "ready", "last_ping": datetime.datetime.now()}
                    print("Worker dict: ")
                    print(self.worker_dict)
                    continue

            else:
                print("ERROR. INVALID REQUEST")
                continue

        print("listen() shutting down")


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)
    # Learn to kill yourself and the workers
    # possibly the answer is process.terminate


if __name__ == '__main__':
    main()

