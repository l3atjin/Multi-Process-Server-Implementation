import os
import logging
import json
import threading
import time
import click
import socket
import mapreduce.utils
import pathlib


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    # Member Variable
    worker_dict = dict()
    def __init__(self, port):
        # Create tmp folder
        # use pathlib with Parents = True and Exist_ok = True
        """ cwd = os.getcwd()
        dir = cwd/'tmp'
        dir.mkdir(parents=True, exist_ok=True) """
        
        # Create new thread - listens for heartbeats on port-1
        # Have a dictionary of workers each having a status
        # in that thread make a socket on port-1
        # for loop that checkes for messages from each workers periodically
            # if after a certain period you dont receive any signal, mark that worker dead
            # upadte the status

        # Create additional threads  (fault tolerance)

        # Create new socket on port number, call listen() 
        thread = threading.Thread(target=self.listen, args=[port])
        thread.start() # This gives up execution to the 'listen' thread
        thread.join() # Wait for listen thread to shut down

        print("main() shutting down")

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")

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
                        """ if worker["status"] == "dead":
                            continue """
                        print("Worker:")
                        print(worker)
                        print(self.worker_dict[worker])
                        # Else Send Shutdown Message
                        sendSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSock.connect(("localhost", self.worker_dict[worker]["worker_port"]))
                        message = json.dumps(context)
                        sendSock.sendall(message.encode('utf-8'))
                        sendSock.close()
                        print("Sent: \n")
                        print(message)
                        print("To worker " + str(self.worker_dict[worker]["worker_port"]))

                    print("Shutting Down.")
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
                    self.worker_dict[worker_pid] = {"worker_port": worker_port, "status": "ready"}
                    print("Worker dict: ")
                    print(self.worker_dict)
                    continue

            else:
                print("ERROR. INVALID REQUEST")
                continue

        print("listen() shutting down")

        # Shutdown
        # if message_dict["message_type"] == "shutdown":
            #for worker in sorted(workers):
            
            #   kill worker
            #kill self

            # Send same shutdown message to workers
                # Worker can complete task, then kill
            # Kill self


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)
    # Learn to kill yourself and the workers
    # possibly the answer is process.terminate


if __name__ == '__main__':
    main()
