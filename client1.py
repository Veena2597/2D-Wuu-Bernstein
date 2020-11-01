import socket
import datetime
import time
import threading
import logging
import random
import pickle
from heapq import *

PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDRESS = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECTED"

logging.basicConfig(filename='client1.log', level=logging.DEBUG, filemode='w')

# Global variables
client_sockets = []
bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bind_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
            if temp.sender == 'P' or temp.sender == 'p':
                balance = balance - temp.amount
            elif temp.receiver == 'P' or temp.receiver == 'p':
                balance = balance + temp.amount
            temp = temp.next
        return balance


def listenTransaction(client_connection, client_address):
    # connection.recv, update the local
    global buffer
    while True:
        msg = client_connection.recv(1024)
        x = pickle.loads(msg)
        logging.debug("[CLIENT MESSAGE] {} : {}".format(client_address, x))
        heappush(buffer, Node(x['timestamp'], x['amount'], x['sender'], x['receiver']))


def inputTransactions():
    global client_sockets
    global buffer
    block = Blockchain()

    while True:
        raw_type = input("Please enter your transaction:")
        s = raw_type.split(' ')
        timestamp = datetime.datetime.now().timestamp()

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
                    print(y)
                    break

            balance = block.traverse()
            print(f"Current Balance: {balance}")


if __name__ == '__main__':

    bind_socket.bind(ADDRESS)
    bind_socket.listen()
    client_sockets = []
    for i in range(1, 4):
        if i != 1:
            connect_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connect_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            connect_socket.settimeout(10)
            try:
                connect_socket.connect_ex((SERVER, 5050 + i))
                client_sockets.append(connect_socket)
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))

    my_transactions = threading.Thread(target=inputTransactions)
    my_transactions.start()

    while True:
        connection, address = bind_socket.accept()
        logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

        listen_transactions = threading.Thread(target=listenTransaction, args=(connection, address))
        listen_transactions.start()

    bind_socket.close()
