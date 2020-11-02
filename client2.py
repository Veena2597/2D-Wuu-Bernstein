import socket
import datetime
import time
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

logging.basicConfig(filename='client1.log', level=logging.DEBUG, filemode='w')

# Global variables
client_sockets = []
bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bind_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
Psocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Psocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
Rsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Rsocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
balance_table = [10, 10, 10]
timetable = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]


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

    def traverse(self, timestamp):
        temp = self.head
        nodelist = []
        while temp:
            if temp.timestamp > timestamp:
                nodelist.append(temp)
            temp = temp.next

        return nodelist


def listenTransaction(client_connection, client_address):
    # connection.recv, update the local
    while True:
        msg = client_connection.recv(1024)
        x = pickle.loads(msg)
        if '5051' in str(client_connection):
            updateTable(x, 0)
            logging.debug("[CLIENT MESSAGE] Table obtained from P")
        if '5053' in str(client_connection):
            updateTable(x, 2)
            logging.debug("[CLIENT MESSAGE] Table obtained from R")
        # heappush(buffer, Node(x['timestamp'], x['amount'], x['sender'], x['receiver']))


def updateTable(new_table, client):
    global timetable
    for i in range(3):
        for j in range(3):
            if timetable[i][j] < new_table[i][j]:
                timetable[i][j] = new_table[i][j]

    for i in range(3):
        if timetable[1][i] < timetable[client][i]:
            timetable[1][i] = timetable[client][i]


def inputTransactions():
    global timetable
    global balance_table
    block = Blockchain()

    while True:
        raw_type = input("Please enter your transaction:")
        s = raw_type.split(' ')
        timestamp = datetime.datetime.now().timestamp()

        if s[0] == 'T' or s[0] == 't':
            logging.debug("[TRANSFER TRANSACTION] {} at {}".format(s, timestamp))
            # tran = {'sender': s[1], 'receiver': s[2], 'amount': s[3], 'timestamp': timestamp}
            if balance_table[1] >= int(s[3]):
                block.push(Node(timestamp, s[3], s[1], s[2]))
                timetable[1][1] = timestamp
                balance_table[1] = balance_table[1] - int(s[3])
                if s[2] == 'P' or s[2] == 'p':
                    balance_table[0] = balance_table[0] + int(s[3])
                elif s[2] == 'R' or s[2] == 'R':
                    balance_table[2] = balance_table[2] + int(s[3])
            else:
                print("INCORRECT TRANSACTION")
            print(timetable)

        elif s[0] == 'B' or s[0] == 'b':
            print(f"Current Balance: {balance_table[1]}")

        elif s[0] == 'M' or s[0] == 'm':
            table = pickle.dumps(timetable)
            if s[1] == 'P' or s[1] == 'p':
                Psocket.sendall(bytes(table))
                nodelist = block.traverse(timetable[0][0])

            elif s[1] == 'R' or s[1] == 'r':
                Rsocket.sendall(bytes(table))
                nodelist = block.traverse(timetable[2][0])


if __name__ == '__main__':

    bind_socket.bind(ADDRESS)
    bind_socket.listen()
    try:
        Psocket.settimeout(10)
        Psocket.connect_ex((SERVER, 5051))
    except socket.error as exc:
        logging.debug("[EXCEPTION] {}".format(exc))

    try:
        Rsocket.settimeout(10)
        Rsocket.connect_ex((SERVER, 5053))
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
