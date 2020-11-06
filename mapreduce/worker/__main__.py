import os
import logging
import threading
import socket
import json
import time
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    isRegistered = False
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd()) 

        # Get workers Process ID
        pid = os.getpid()
        thread_hb = threading.Thread(target=self.send_hb, args=[master_port, worker_port])
        # Create TCP Socket on worker_port, call listen()
        thread = threading.Thread(target=self.listen, args=[master_port, worker_port])
        thread.start() # This gives up execution to the 'listen' thread
        # call functions
        thread.join() # Wait for listen thread to shut down
        thread_hb.join()
        # Send register to master 
        # When receive register_ack, create new thread
            # Sends heartbeat messages to master 

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", worker_port))
        sock.listen()

        # Send hb Message to Master
        context = {
            "message_type": "heartbeat",
            "worker_pid": int
        }
        while True:
            send_message(master_port, context)
            sleep(2)


    def listen(self,master_port, worker_port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", worker_port))
        sock.listen()

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
            
                # If Shutdown Message
                elif message_dict["message_type"] == "shutdown":
                    # shutdown()
                    # kill self
                    print("Shutting Down.")
                    clientsocket.close()
                    return

                elif message_dict["message_type"] == "new_worker_job":
                    # Master  Job Shit
                    continue
                

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
