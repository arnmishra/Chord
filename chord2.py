import collections
import sys
import socket
import time
import ast
import Queue
import pickle
import copy
import random
from threading import Lock,Thread, Condition

'''
Change the code to include finger table update implementation.
Send from concatenate to predecessor to find each node and build the finger table the normal way
Add a flag to update finger tables to make a removal node too 
'''

show_all_queue = Lock()
cond = Condition()
cond2 = Condition()
queue_mutex = Lock()
nodes_lock = Lock()
def main(config):
    parts = parse_file(config)
    global min_delay
    global max_delay
    min_delay = float(parts[0][0])/1000
    max_delay = int(parts[0][1])/1000
    initial_port = parts[1]
    client_thread = Thread(target=client, args=(initial_port)) #start client
    client_thread.daemon = True
    client_thread.start()
    global num_nodes
    num_nodes = 0
    
    global zero
    zero = False
    zero = get_socket(5000, False)
    while not zero:
        zero = get_socket(5000, False)
    
    while True:
        time.sleep(1)

def parse_file(config):
    parts = []
    with open(config) as f:
        for line in f:
            if(len(line.split()) != 0):
                parts.append(line.split())
    return parts

def client(initial_port):
    global go
    global show_all
    global crash
    crash = 0
    show_all = False
    go = 1
    global nodes # hash node_id to the socket
    nodes = {}
    print_thread = Thread(target = start_printing, args=([5000 - 1])) #start printing from the client
    print_thread.daemon = True
    print_thread.start()

    port = int(initial_port) #Start Node 0 on  initial port
    node_thread = Thread(target=start_node, args=([port])) #start each node
    node_thread.daemon = True
    node_thread.start()
    nodes[0] = get_socket(port, False)
    while (not nodes[0]):
        nodes[0] = get_socket(port, False) # Continue trying to connect till it connects
    nodes[0].sendall("join\n")

    while True:
        while not go:
            time.sleep(1)
        go = 0

        message = raw_input('')

        command = message.split()[0]
        try:
            node_id = message.split()[1] #i.e. Node "18"
            if(node_id != "all"):
                node_id = int(node_id)
        except:
            print "Invalid message"
            go = 1
            continue
            node_id = -1

        if command == "join": #join p

            if(node_id in nodes):
                print "Node already exists"
                go = 1
                continue
            if(int(node_id) > 255):
                print "Node id must be less than 255"
                go = 1
                continue

            port = int(node_id) + int(initial_port) #i.e. Node 18 is on port 8018

            node_thread = Thread(target=start_node, args=([port])) #start each node
            node_thread.daemon = True
            node_thread.start()

            nodes[node_id] = get_socket(port, False)
            while (not nodes[node_id]):
                nodes[node_id] = get_socket(port, False) # Continue trying to connect till it connects
            nodes = collections.OrderedDict(sorted(nodes.items())) #Sort the nodes in increasing order
            nodes[node_id].sendall("join\n")
            
        elif command == "find": #find p k

            nodes[node_id].sendall(message + "\n") #send the key that is being searched for

        elif command == "crash": #crash p
            if node_id in nodes:
                crash = node_id
                nodes[node_id].sendall("crash\n") #tell the node to crash
                del nodes[node_id] #remove the node from the list of connections
            else:
                print "This node does not exist."
                go = 1
                continue

        elif command == "show": #show p

            if(node_id == "all"):
                nodes = collections.OrderedDict(sorted(nodes.items()))
                for socket in nodes:
                    nodes[socket].sendall("show all\n") #send the key that is being searched for
                    #with cond:
                    cond.acquire()
                    cond.wait()
                    cond.release()
                #print "setting show_all"
            else:
                if(node_id in nodes):
                    nodes[node_id].sendall("show\n") #send the key that is being searched for
                else:
                    print "N" + node_id + " isn't a valid Node"
                    go = 1

        else:
            print "Invalid message"
            go = 1
        #print "write",show_all

def start_printing(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe

    while True:
        conn, addr = s.accept()
        print_thread = Thread(target = print_from_conns, args= ([conn]))
        print_thread.daemon = True
        print_thread.start()

def print_from_conns(conn):
    global go, show_all, show_all_nodes
    show_all_nodes = Queue.PriorityQueue()
    while True:
        data = conn.recv(4096)
        if(len(data.split('\n')) == 7):
            contains_all = data.split('\n')[6].strip()
            if(contains_all == "all"):
                contains_all = True
        else:
            contains_all = False
        if(data == ""):
            continue
        else:
            if "FingerTable:" not in data.strip():
                data = data.replace('\n', '')
                print(data)
            else:
                if(contains_all == True):
                    message = data.split('\n')
                    data = message[1] + '\n' + message[2] + '\n' + message[3] + '\n' + message[4] + '\n' + message[5]
                    print("========Node " + message[0] + "======== ")
                    print(data)
                    print("\n")
                    cond.acquire()
                    cond.notifyAll()
                    cond.release()
                else:
                    print "data", data
         
            go = 1

def get_socket(port, finger_table):
    global zero
    global crash
    if int(port) == 5000 and zero and not crash:
        return zero
    elif int(port) == 5000 and zero and crash:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.connect(("127.0.0.1", int(port)))
            zero = s
            crash = 0
            return s
        except:
            return False

    if finger_table:
        for i in range(8):
            if int(finger_table[i][0]) == int(port)-5000 and finger_table[i][1] != "self":
                return finger_table[i][1] #if the socket has already been made, return
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect(("127.0.0.1", int(port)))
        return s
    except:
        return False

def start_node(port):
    global num_nodes
    keys = [] #list of all keys in this node
    new_table = [0 for x in range(8)] #maps node_ids to sockets
    conns = []
    hello = 5
    predecessor_id = 0
    count = 0
    super_successor = 0

    if(port == 5000):
        keys = [x for x in range(256)]
        predecessor_keys = [x for x in range(256)]

    client = get_socket(5000 - 1, False) # get socket to client
    while not client:
        client = get_socket(5000 - 1, False)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe
    q = Queue.Queue()
    q2 = Queue.Queue()

    node_id = port - 5000

    accept_thread = Thread(target = start_accepting, args= (s, q, q2, node_id, conns))
    accept_thread.daemon = True
    accept_thread.start()

    while True:
        data = q.get()

        if(data.split()[0] == "predecessor"): #predecessor predecessor_id (crash old_id initial_id) send_keys<-- if coming from the crash process
            old_id = -1
            predecessor_id = int(data.split('\r')[1])
            if(data.split('\r')[2] == "crash"):
                old_id = int(data.split('\r')[3])
                initial_id = int(data.split('\r')[4])
                a = 4
            else:
                a = 2
            predecessor_keys = []
            for i in range(a,len(data.split())):
                predecessor_keys.append(int(data.split()[i]))
            sock = get_socket(5000 + predecessor_id, finger_table)
            sock.sendall("super_successor " + str(finger_table[0][0]) + '\n')
            if old_id != -1:
                fix_table(initial_id, finger_table, node_id, old_id)
                if initial_id != finger_table[0][0]:
                    finger_table[0][1].sendall("fix " + str(initial_id) + " " + str(old_id) + "\n")

        elif(data.split()[0] == "fix"):
            initial_id = int(data.split()[1])
            old_id = int(data.split()[2])
            fix_table(initial_id, finger_table, node_id, old_id)
            if initial_id != finger_table[0][0]:
                finger_table[0][1].sendall("fix " + str(initial_id) + " " + str(old_id) + "\n")

        elif(data.split()[0] == "super_successor"): #super_successor id
            super_successor = data.split()[1]

        elif(data == "crash"): #crash
            for finger in finger_table:
                try:
                    finger[1].shutdown(socket.SHUT_RDWR)
                    finger[1].close()
                except:
                    i = 0
            #for conn in conns:
               #conn.close()
            client.sendall("P" + str(node_id) + " Crashed\n")
            nodes_lock.acquire()
            num_nodes -= 1
            nodes_lock.release()

        elif("show" in data): #show
            line = "N" + str(node_id) + "\n"
            temp = []
            for i in range(8):
                temp.append(finger_table[i][0])
            line +=  "FingerTable: " + str(temp) + "\n"
            line +=  "Keys: " + str(keys) + "\n"
            line += "predecessor_keys: " + str(predecessor_keys) + "\n"
            line += "super_successor: " + str(super_successor) + "\n"
            line += "predecessor_id: " + str(predecessor_id)
            if("all" in data):
                line += "\nall"
            client.sendall(line)

        elif(data.split()[0] == "find"): #find p k (i) (original_id)
            key = int(data.split()[2])
            if key in keys:
                if(len(data.split()) == 3): # For a regular find operation
                    client.sendall(str(node_id) + "\n")
                elif(len(data.split()) == 4): # For a finger table initialization find function (using i)
                    sock = get_socket(5000, finger_table)
                    string = "combine " + data.split()[3] + " " + str(node_id) + " " + str(key) #sends i, i'th member of finger table, and key
                    sock.sendall(string + "\n")
                else: # For finding a new node after a crash (using i and original_id)
                    sock = get_socket(int(data.split()[4]) + 5000, finger_table)
                    sock.sendall("combine " + data.split()[3] + " " + str(node_id) + " crash\n")
            else:
                print finger_table
                found = find(key, finger_table, node_id)
                if found[1] == "self":
                    s = get_socket(5000 + int(found[0]), False)
                else:
                    s = found[1]
                s.sendall(data + '\n')

        elif(data.split()[0] == "finger"): #finger new_finger keys predecessor_id
            predecessor_id = int(data.split()[3])
            new_finger = int(data.split()[1])
            if(new_finger != node_id):
                finger_table = update_fingers(node_id, new_finger, finger_table)
                q2.put(finger_table[0][0])
            keys = set_keys(finger_table, predecessor_id, node_id)
            if finger_table[0][0] == new_finger:
                client.sendall("P" + str(new_finger) + " Joined" + "\n")

        elif(data.split()[0] == "keys"): #keys predecessor_id
            predecessor_id = int(data.split()[1])
            keys = set_keys(finger_table, predecessor_id, node_id)

        elif(data.split()[0] == "join"): #join
            #INIT Finger Table
            if(int(node_id) == 0):
                finger_table = [[0, "self"] for x in range(8)]
                client.sendall("P0 Joined\n")
                nodes_lock.acquire()
                num_nodes += 1
                nodes_lock.release()
                heartbeat_thread = Thread(target=send_heartbeats, args=(finger_table[0], node_id))
                heartbeat_thread.daemon = True
                heartbeat_thread.start()
            else:
                sock = get_socket(5000, False)
                sock.sendall("init " + str(node_id) + "\n")

        elif(data.split()[0] == "init"): #init p
            search_id = int(data.split()[1])
            for i in range(8):
                val = (search_id + 2**i) % 255
                string = "find " + str(finger_table[0][0]) + " " + str(val) + " " + str(i) + '\n'
                sock = finger_table[0][1]
                if(sock == "self"):
                    sock = get_socket(5000, finger_table)
                sock.sendall(string + "\n")

        elif(data.split()[0] == "combine"): #initializing finger table, combine index i'th_finger_key; or updating finger table for crashed node
            i = int(data.split()[1])
            finger = data.split()[2]
            key = data.split()[3]
            if(key == "crash"): #combine index crash
                finger_table[i][0] = int(finger)
                finger_table[i][1] = get_socket(int(finger) + 5000, False)
            else:
                new_table[i] = finger
                count += 1
                if count == 8:
                    count = 0
                    port = 5000 + (int(key) - 2**i) % 255
                    sock = get_socket(port, finger_table)
                    sock.sendall(str(new_table) + "\n")

        elif(data == "change successor"):
            print "node", node_id
            old_id = finger_table[0][0]
            sock = get_socket(int(super_successor) + 5000, False)
            for finger in finger_table:
                if finger[0] == old_id:
                    finger[0] = int(super_successor)
                    #finger[1].close()
                    finger[1] = sock
            send_keys = ""
            for i in range(len(keys)):
                send_keys += str(keys[i]) + " "
            finger_table[0][1].sendall("concatenate " + str(node_id) + " " + str(old_id) + " " + send_keys + "\n")
            sock = get_socket(predecessor_id + 5000, finger_table)
            sock.sendall("super_successor " + str(finger_table[0][0]) + "\n")


        elif(data.split()[0] == "concatenate"):
            keys += predecessor_keys
            keys.sort()
            predecessor_id = int(data.split()[1])
            old_id = int(data.split()[2])
            fix_table(predecessor_id, finger_table, node_id, old_id) #fixes this nodes table to remove the old id
            predecessor_keys = []
            for i in range(3,len(data.split())):
                predecessor_keys.append(int(data.split()[i]))
            sock = get_socket(predecessor_id + 5000, finger_table)
            sock.sendall("super_successor " + str(finger_table[0][0]) + "\n")
            send_keys = ""
            for i in range(len(keys)):
                send_keys += str(keys[i]) + " "
            finger_table[0][1].sendall("predecessor\r" + str(predecessor_id) + "\rcrash\r" + str(old_id) + "\r" + str(predecessor_id) + "\r" + send_keys + "\n")


        else:
            predecessor_keys = []
            finger_table = ast.literal_eval(data)
            successor = 1
            prev = -1
            for i in range(8):
                if(len(finger_table[i]) == 2 and finger_table[i][0] != node_id and finger_table[i][1] == "self"): #Deal with case where 0,"self" is a node in the finger_table from init
                    finger_table[i] = int(finger_table[i][0])

                if(node_id == 0):
                    break

                elif(finger_table[i] == node_id):
                    finger_table[i] = [node_id, "self"]

                else:
                    if finger_table[i] != prev:
                        s = get_socket(int(finger_table[i]) + 5000, finger_table)
                    if not s:
                        print "Could not connect to " + str(finger_table[i])
                    
                    finger_table[i] = [int(finger_table[i]), s]
                    
                    if(successor): # Send the node_id to the successor to update the finger_table and the keys flag to update the keys
                        finger_table[i][1].sendall("finger " + str(node_id) + " keys " + str(node_id) + "\n")
                        successor = 0

                prev = finger_table[i][0]
            
            cond2.acquire()
            cond2.notifyAll()
            cond2.release()
            nodes_lock.acquire()
            num_nodes += 1
            nodes_lock.release()
            #Start heartbeat messages
            heartbeat_thread = Thread(target=send_heartbeats, args=(finger_table[0], node_id))
            heartbeat_thread.daemon = True
            heartbeat_thread.start()

def fix_table(initial_id, finger_table, node_id, old_id):
    if node_id != initial_id:
        for i in range(8):
            if finger_table[i][0] == old_id: #need to fix this finger
                key = (node_id + 2**i) % 255
                if(i == 0):
                    finger_table[7][1].sendall("find " + str(finger_table[i][0]) + " " + str(key) + " " + str(i) + " " + str(node_id) + "\n")
                else:
                    finger_table[i-1][1].sendall("find " + str(finger_table[i+1][0]) + " " + str(key) + " " + str(i) + " " + str(node_id) + "\n")

#Send heartbeats to successor
def send_heartbeats(successor, node_id):
    global crash
    if(num_nodes <= 1):
        with cond2:
            cond2.wait()
    while successor[1] == "self":
        i = 0
    successor[1].settimeout(5)
    time_t = str(time.time())
    time.sleep(2.5)
    successor[1].sendall("heartbeat message start " + time_t + " " + str(node_id) + "\n")
    while True:
        data = successor[1].recv(1024)
        if not data:
            #print("N" + str(node_id) + "'s Successor Crashed")
            if node_id != int(crash):
                me = get_socket(5000 + node_id, False)
                print "node", node_id
                print me
                me.sendall("change successor\n")
            else:
                print "returning1",node_id
                return
        if("end heartbeat" in data):
            time_t = str(time.time())
            try:
                successor[1].sendall("heartbeat message start " + time_t + " " + str(node_id) + "\n")
            except:
                print "returning2",node_id
                return

def set_keys(finger_table, predecessor_id, node_id):
    if(predecessor_id > node_id):
        keys = [x for x in range(0, node_id + 1)]
        keys += (x for x in range(predecessor_id + 1, 256))
    elif(predecessor_id < node_id):
        keys = [x for x in range(predecessor_id + 1, node_id + 1)]
    else:
        print "Setting Incorrect Keys" #Should never print
    send_keys = ""
    for i in range(len(keys)):
        send_keys += str(keys[i]) + " "
    finger_table[0][1].sendall("predecessor\r" + str(node_id) + "\r" + send_keys + "\n")
    return keys

def find(value, finger_table, node_id):
    successor = 1
    prev_node = -1
    sent = 0
    finger = 0
    for i in range(8):
        if(finger_table[i][0] == node_id):
            successor = 0
            continue
        elif successor and finger_table[i][0] >= value and node_id < value: #the successor is the node you are looking for because the value is between this node and the successor
            finger = finger_table[i] #return successor's id and socket if it is the chosen one
            sent = 1
            break
        if finger_table[i][0] > value:
            sent = 1
            finger = finger_table[i-1] #return the largest node that is smaller than the value
        successor = 0
    if not sent:
        finger = finger_table[i] #if none of them are greater, then just jump to the last node in the table because thats the closest one
    return finger

def start_accepting(s, q, q2, node_id, conns):
    while True:
        conn, addr = s.accept()
        conns.append(conn)
        read_thread = Thread(target = read_from_conns, args= (conn, q, q2, node_id))
        read_thread.daemon = True
        read_thread.start()

def read_from_conns(conn, q, q2, node_id):
    global max_delay
    global min_delay
    while True:
        #Goes into except if node crashes and connection is closed
        try:
            data = conn.recv(4096)
            commands = data.split("\n")
            if not commands[0] == "join" and not "show" in commands[0]:
                time.sleep(random.randrange(min_delay, max_delay))
        except:
            me = get_socket(5000 + node_id, False)
            print "node", node_id
            print me
            me.sendall("change successor\n")
            print "returning4",node_id
            return
        for command in commands:
            if command == "":
                continue
            if("heartbeat" in command):
                i = 0
                if("start" in command):
                    time_received = command.split()[3]
                    try:
                        successor = -1
                        while not q2.empty():
                            successor = q2.get()
                        conn.sendall("end heartbeat " + str(time_received) + " " + str(time.time()) + " " + str(successor) + "\n")
                    except:
                        print "returning3",node_id
                        return
                continue
            else:
                q.put(command)

def update_fingers(node_id, new_finger, finger_table):
    s = False
    successor = 1
    for i in range(8):
        if (new_finger < finger_table[i][0] or finger_table[i][0] == 0) and new_finger >= (node_id + 2 ** i)%255:
            if not s:
                s = get_socket(new_finger + 5000, finger_table)
                if not s:
                    print "Could not connect to " + str(new_finger)
            finger_table[i][0] = new_finger
            finger_table[i][1] = s

        if(finger_table[i][0] != node_id and successor):
            if new_finger != node_id:
                finger_table[i][1].sendall("finger " + str(new_finger) + " keys " + str(node_id) + "\n") # Send the finger and key flags to the successor (assuming its not the original node)       
            successor = 0
    return finger_table



if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print('Usage: python %s <config file name>' % sys.argv[0]) #usage
        print('join p = create node with node_id p')
        print('show p = show fingertable, keys, and id for node_id p')
        print('show all = show fingertable, keys, and id for all nodes')
        print('find p k = find key k from node p')
        print('crash p = crashes node with node_id p')
        exit(1)
    main(sys.argv[1])