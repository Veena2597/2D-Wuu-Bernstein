import socket
import datetime
import threading
import logging

PORT = 5050
CLOCK_SERVER = socket.gethostbyname(socket.gethostname())
ADDRESS = (CLOCK_SERVER, PORT)
FORMAT = 'utf-8'
CLOCK_REQUEST = "SYNCHRONIZE"
logging.basicConfig(filename='clock_server.log', level=logging.DEBUG, filemode='w')


def sendTime(client_connection, client_address):
    connected = True
    while connected:
        # Client sends CLOCK_REQUEST whenever it needs to know the time from the server
        msg = client_connection.recv(1024).decode(FORMAT)
        if msg == CLOCK_REQUEST:
            client_connection.send(str(datetime.datetime.now()).encode(FORMAT))
    client_connection.close()


if __name__ == '__main__':
    # Creating the clock server socket
    clock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    logging.debug("[CLOCK SERVER] Successfully created")

    # Binding the clock server socket to the address - defined globally
    clock.bind(ADDRESS)

    # The clock server keeps listening for different clients
    clock.listen()
    logging.debug("[CLOCK SERVER] Clock server is listening")

    while True:
        # Clock server accepts new clients and creates a new thread for each client
        connection, address = clock.accept()
        logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))
        thread = threading.Thread(target=sendTime, args=(connection, address))
        thread.start()

    clock.close()
