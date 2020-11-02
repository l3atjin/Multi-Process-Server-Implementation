import os
import logging
import json
import threading
import time
import click
import socket
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        # Create tmp folder
        # Create new thread - listens for heartbeats
        # Create additional threads  (fault tolerance)

        # Create new socket on port number, call listen() 
        thread = threading.Thread(target=listen, args=(port,))
        thread.start()
        time.sleep(100) # This gives up execution to the 'listen' thread
        thread.join()  # Wait for listen thread to shut down
        print("main() shutting down")
        

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
            "worker_pid": 77811
        }
        logging.debug("Master:%s received\n%s",
            port,
            json.dumps(message_dict, indent=2),
        )
        
        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")
        time.sleep(120)

def listen(port):
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
        print("listening")

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
        # If Shutdown Message
        if message_dict['message_type']:
            if message_dict["message_type"] == "shutdown":
                # shutdown()
                print("Shutting Down.")
                clientsocket.close()
                return
                
            elif message_dict["message_type"] == "new_master_job":
                # Master  Job Shit

            elif message_dict["message_type"] == "register_ack":
                # Register Ack Shit
            
        else:
            print("ERROR. INVALID REQUEST")
            continue 
        print(message_dict)

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


if __name__ == '__main__':
    main()
