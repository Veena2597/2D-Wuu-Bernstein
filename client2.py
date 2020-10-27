import socket
import datetime
import time
from dateutil import parser
from timeit import default_timer
import threading
import logging
import random
import pickle
from heapq import *

PORT = 5052
SERVER = socket.gethostbyname(socket.gethostname())
ADDRESS = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECTED"
CLOCK_REQUEST = "SYNCHRONIZE"

logging.basicConfig(filename='client2.log', level=logging.DEBUG, filemode='w')

# Global variables
clock_server_time = datetime.datetime.now()
client_time_at_sync = datetime.datetime.now()
client_sockets = []
bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bind_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
clock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
buffer = []


class Node:
    def __init__(self, timestamp, amount, sender, receiver):
        self.amount = int(amount)
        self.sender = sender
        self.receiver = receiver
        self.timestamp = timestamp
        self.next = None

    def __lt__(self, other):
        # min heap based on job.end
        return self.timestamp < other.timestamp


class Blockchain:
    def __init__(self):
        self.head = None

    def push(self, node):

        if self.head is None:
            self.head = node
            return
        last = self.head
        while last.next:
            last = last.next
        last.next = node

    def traverse(self):
        temp = self.head
        balance = 10
        while temp:
            if temp.sender == 'Q' or temp.sender == 'q':
                balance = balance - temp.amount
            elif temp.receiver == 'Q' or temp.receiver == 'q':
                balance = balance + temp.amount
            temp = temp.next
        return balance


def clientClock():
    global clock_server_time
    global client_time_at_sync
    current_sys_time = datetime.datetime.now().timestamp()
    current_sim_time = client_time_at_sync.timestamp() + (current_sys_time - clock_server_time.timestamp()) * 1.5
    return current_sim_time


def synchronizeTime():
    global clock_server_time
    global client_time_at_sync

    # Client sends CLOCK_REQUEST to the clock server and the request time is recorded
    while True:
        request_time = clientClock()
        clock_socket.send(CLOCK_REQUEST.encode(FORMAT))
        logging.debug("[CLOCK SERVER] Requested time from server at {}".format(request_time))

        # Client receives time from the clock server and the response time is recorded
        clock_time = parser.parse(clock_socket.recv(1024).decode(FORMAT))
        response_time = clientClock()
        clock_server_time = clock_time
        logging.debug("[CLOCK SERVER] Response from server at {}".format(response_time))

        actual_time = datetime.datetime.fromtimestamp(response_time)
        logging.debug("[CLOCK SERVER] Time received from clock server {}".format(str(clock_server_time)))

        # Delay between request sent and response received is calculated
        delay = response_time - request_time
        logging.debug("[CLIENT CLOCK] Delay in response from clock server is {}s".format(str(delay)))

        # Times before and after synchronization are printed
        logging.debug("[CLIENT CLOCK] Time before synchronization {}s".format(str(actual_time)))

        # Calculated with Cristian's algorithm
        client_time_at_sync = clock_server_time + datetime.timedelta(seconds=(delay) / 2)
        logging.debug("[CLIENT CLOCK] Time after synchronization {}s".format(str(client_time_at_sync)))

        error = actual_time - client_time_at_sync
        logging.debug("[CLIENT CLOCK] Synchronization error {}s".format(str(error.total_seconds())))

        # The client ping the clock again after 20s
        time.sleep(20)
        # return client_time_at_sync


def listenTransaction(client_connection, client_address):
    # connection.recv, update the local
    global buffer
    while True:
        msg = client_connection.recv(1024)
        x = pickle.loads(msg)
        logging.debug("[CLIENT MESSAGE] {} : {}".format(client_address, x))
        heappush(buffer, Node(x['timestamp'], x['amount'], x['sender'], x['receiver']))


def inputTransactions():
    global client_time_at_sync
    global client_sockets
    global buffer
    block = Blockchain()

    while True:
        raw_type = input("Please enter your transaction:")
        s = raw_type.split(' ')
        timestamp = clientClock()

        if s[0] == 'T' or s[0] == 't':
            logging.debug("[TRANSFER TRANSACTION] {} at {}".format(s, timestamp))
            tran = {'sender': s[1], 'receiver': s[2], 'amount': s[3], 'timestamp': timestamp}
            b = pickle.dumps(tran)

            time.sleep(20 + random.randint(1, 6))

            while len(buffer) > 0:
                y = heappop(buffer)
                if y.timestamp <= timestamp:
                    block.push(y)
                else:
                    heappush(buffer, y)
                    break

            balance = block.traverse()
            if balance >= int(s[3]):
                heappush(buffer, Node(timestamp, s[3], s[1], s[2]))
                print("SUCCESS")
                for sock in client_sockets:
                    sock.sendall(bytes(b))
                logging.debug("[BROADCAST TRANSACTION] {}".format(s))
            else:
                print("INCORRECT")

        elif s[0] == 'B' or s[0] == 'b':
            time.sleep(20 + random.randint(1, 6))
            while len(buffer) > 0:
                y = heappop(buffer)
                if y.timestamp <= timestamp:
                    block.push(y)
                else:
                    heappush(buffer, y)
                    break

            balance = block.traverse()
            print(f"Current Balance: {balance}")


if __name__ == '__main__':

    bind_socket.bind(ADDRESS)
    bind_socket.listen()
    try:
        clock_socket.settimeout(10)
        clock_socket.connect((SERVER, 5050))
    except socket.error as exc:
        logging.debug("[EXCEPTION] {}".format(exc))
    client_sockets = []
    for i in range(1, 4):
        if i != 2:
            connect_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connect_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            connect_socket.settimeout(10)
            try:
                connect_socket.connect_ex((SERVER, 5050 + i))
                client_sockets.append(connect_socket)
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))

    clock_thread = threading.Thread(target=synchronizeTime)
    clock_thread.start()
    my_transactions = threading.Thread(target=inputTransactions)
    my_transactions.start()

    while True:
        connection, address = bind_socket.accept()
        logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

        listen_transactions = threading.Thread(target=listenTransaction, args=(connection, address))
        listen_transactions.start()

    clock_socket.close()
    bind_socket.close()
