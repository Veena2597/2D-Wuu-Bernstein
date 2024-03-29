import socket
import threading
import logging
import pickle

PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDRESS = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECTED"

logging.basicConfig(filename='client1.log', level=logging.DEBUG, filemode='w')

# Global variables
bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bind_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
Qsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Qsocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
Rsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Rsocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
balance_table = [10, 10, 10]
timetable = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
logical_clock = 0
min_events = [0, 0, 0]


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

    def traverse(self, table, client):
        temp = self.head
        nodelist = []
        while temp:
            if temp.sender == 'P' or temp.sender == 'p':
                if temp.timestamp > table[client][0]:
                    nodelist.append(temp)
            elif temp.sender == 'Q' or temp.sender == 'q':
                if temp.timestamp > table[client][1]:
                    nodelist.append(temp)
            elif temp.sender == 'R' or temp.sender == 'r':
                if temp.timestamp > table[client][2]:
                    nodelist.append(temp)
            temp = temp.next
        return nodelist

    def remove(self, timestamp, sender):
        temp = self.head
        prev = None
        while temp and temp.timestamp <= timestamp and temp.sender.lower() == sender.lower():
            self.head = temp.next
            temp = temp.next
        while temp:
            while temp and (temp.timestamp > timestamp or temp.sender.lower() != sender.lower()):
                prev = temp
                temp = temp.next
            if temp is None:
                return
            prev.next = temp.next
            temp = prev.next

    def printChain(self):
        print_list = []
        temp = self.head
        while temp:
            print_list.append([temp.sender, temp.receiver, temp.amount])
            temp = temp.next
        logging.debug("Events in local log: {}".format(print_list))


def listenTransaction(client_connection, client_address):
    # connection.recv, update the local
    while True:
        msg = client_connection.recv(1024)
        x = pickle.loads(msg)
        updateBalance(x['log'])
        updateTable(x['table'], x['client'])
        garbageCollect()
        logging.debug("[CLIENT MESSAGE] Table obtained from {}".format(str(client_address)))
        logging.debug("[TIMETABLE] {}".format(str(timetable)))
        logging.debug("[BALANCE TABLE] {}".format(str(balance_table)))


def updateTable(new_table, client):
    global timetable
    for i in range(3):
        for j in range(3):
            if timetable[i][j] < new_table[i][j]:
                timetable[i][j] = new_table[i][j]

    for i in range(3):
        if timetable[0][i] < timetable[client][i]:
            timetable[0][i] = timetable[client][i]


def updateBalance(nodelist):
    global balance_table
    global block
    for node in nodelist:
        node.next = None
        if (node.sender == 'Q' or node.sender == 'q') and node.timestamp > timetable[0][1]:
            balance_table[1] = balance_table[1] - int(node.amount)
            block.push(node)
            if node.receiver == 'P' or node.receiver == 'p':
                balance_table[0] = balance_table[0] + int(node.amount)
            elif node.receiver == 'R' or node.receiver == 'r':
                balance_table[2] = balance_table[2] + int(node.amount)
        elif (node.sender == 'R' or node.sender == 'r') and node.timestamp > timetable[0][2]:
            balance_table[2] = balance_table[2] - int(node.amount)
            block.push(node)
            if node.receiver == 'P' or node.receiver == 'p':
                balance_table[0] = balance_table[0] + int(node.amount)
            elif node.receiver == 'Q' or node.receiver == 'q':
                balance_table[1] = balance_table[1] + int(node.amount)


def garbageCollect():
    global timetable
    global block
    global min_events

    for i in range(3):
        x = timetable[0][i]
        for j in range(3):
            if x > timetable[j][i]:
                x = timetable[j][i]
        if x > min_events[i]:
            block.remove(x, chr(ord('p') + i))
            min_events[i] = x
    block.printChain()


def inputTransactions():
    global timetable
    global balance_table
    global block
    global logical_clock

    while True:
        raw_type = input("Please enter your transaction:")
        s = raw_type.split(' ')

        if s[0] == 'T' or s[0] == 't':
            logical_clock = logical_clock + 1
            logging.debug("[TRANSFER TRANSACTION] {} at {}".format(s, logical_clock))
            if balance_table[0] >= int(s[3]):
                block.push(Node(logical_clock, s[3], s[1], s[2]))
                timetable[0][0] = logical_clock
                balance_table[0] = balance_table[0] - int(s[3])
                if s[2] == 'Q' or s[2] == 'q':
                    balance_table[1] = balance_table[1] + int(s[3])
                elif s[2] == 'R' or s[2] == 'R':
                    balance_table[2] = balance_table[2] + int(s[3])
            else:
                print("INCORRECT TRANSACTION")
            logging.debug("[TIMETABLE] {}".format(str(timetable)))

        elif s[0] == 'B' or s[0] == 'b':
            print(f"Current Balance: {balance_table[0]}")
            print(f"Balance Table: {balance_table}")
            logging.debug("[TIMETABLE] {}".format(str(timetable)))
            logging.debug("[BALANCE TABLE] {}".format(str(balance_table)))

        elif s[0] == 'M' or s[0] == 'm':
            if s[1] == 'Q' or s[1] == 'q':
                nodelist = block.traverse(timetable, 1)
                table = pickle.dumps({'table': timetable, 'client': 0, 'log': nodelist})
                Qsocket.sendall(bytes(table))
                print("MESSAGE SENT TO CLIENT Q FROM P")

            elif s[1] == 'R' or s[1] == 'r':
                nodelist = block.traverse(timetable, 2)
                table = pickle.dumps({'table': timetable, 'client': 0, 'log': nodelist})
                Rsocket.sendall(bytes(table))
                print("MESSAGE SENT TO CLIENT R FROM P")


if __name__ == '__main__':
    block = Blockchain()
    bind_socket.bind(ADDRESS)
    bind_socket.listen()
    try:
        #Qsocket.settimeout(10)
        Qsocket.connect_ex((SERVER, 5052))
    except socket.error as exc:
        logging.debug("[EXCEPTION] {}".format(exc))

    try:
        #Rsocket.settimeout(10)
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
