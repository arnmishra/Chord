import collections
import sys
import socket
import time
import ast
import Queue
import pickle
from threading import Lock,Thread, Condition

show_all_queue = Lock()
cond = Condition()
queue_mutex = Lock()
def main(config):
    parts = parse_file(config)

    min_delay = parts[0][0]
    max_delay = parts[0][1]
    initial_port = parts[1]
    client_thread = Thread(target=client, args=(initial_port)) #start client
    #print "Created Client Thread"
    client_thread.daemon = True
    client_thread.start()

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
    #print "the world has ended"
    show_all = False
    go = 1
    global nodes # hash node_id to the socket
    nodes = {}
    print_thread = Thread(target = start_printing, args=([8000 - 1])) #start printing from the client
    #print "Created Start Print Thread"
    print_thread.daemon = True
    print_thread.start()

    port = int(initial_port) #Start Node 0 on  initial port
    node_thread = Thread(target=start_node, args=([port])) #start each node
    #print "Created Node Thread"
    node_thread.daemon = True
    node_thread.start()
    nodes['0'] = get_socket(port, False)
    while (not nodes['0']):
        nodes['0'] = get_socket(port, False) # Continue trying to connect till it connects
    #nodes['0'].sendall(get_fingers('0', nodes))
    nodes['0'].sendall("join\n")

    while True:
        while not go:
            time.sleep(1)
        go = 0

        message = raw_input('')

        command = message.split()[0]
        try:
            node_id = message.split()[1] #i.e. Node "18"
        except:
            print "Invalid message"
            go = 1
            continue
            node_id = -1

        if command == "join": #join p

            if(node_id in nodes):
                print "Node already exists"
                continue
            if(int(node_id) > 255):
                print "Node id must be less than 255"
                continue

            port = int(node_id) + int(initial_port) #i.e. Node 18 is on port 8018

            node_thread = Thread(target=start_node, args=([port])) #start each node
            #print "Created node thread"
            node_thread.daemon = True
            node_thread.start()

            nodes[node_id] = get_socket(port, False)
            while (not nodes[node_id]):
                nodes[node_id] = get_socket(port, False) # Continue trying to connect till it connects
            nodes = collections.OrderedDict(sorted(nodes.items())) #Sort the nodes in increasing order
            #nodes[node_id].sendall(get_fingers(node_id, nodes))
            nodes[node_id].sendall("join\n")
            
        elif command == "find": #find p k

            nodes[node_id].sendall(message + "\n") #send the key that is being searched for

        elif command == "crash": #crash p

            nodes[node_id].sendall("crash\n") #tell the node to crash
            del nodes[node_id] #remove the node from the list of connections

        elif command == "show": #show p

            if(node_id == "all"):
                for socket in nodes:
                    nodes[socket].sendall("show all\n") #send the key that is being searched for
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
        #print "Created print thread"
        print_thread.daemon = True
        print_thread.start()

def print_from_conns(conn):
    global go, show_all, show_all_nodes
    show_all_nodes = Queue.PriorityQueue()
    while True:
        data = conn.recv(2048)
        node_id = data.split('\n')[0][1:].strip()
        if(len(data.split('\n')) == 4):
            contains_all = data.split('\n')[3].strip()
            if(contains_all == "all"):
                contains_all = True
        else:
            contains_all = False
        #print(node_id, contains_all)
        if(data == ""):
            continue
        else:
            if "FingerTable:" not in data.strip():
                data = data.replace('\n', '')
                #print "The earth"
                print(data)
            else:
                if(contains_all == True):
                    message = data.split('\n')
                    data = message[0] + '\n' + message[1] + '\n' + message[2]
                    node_id = int(node_id)
                    show_all_nodes.put((node_id, data))
                    #print(str(node_id) + '\n')
                    if(show_all_nodes.qsize() == len(nodes)):
                        cond.acquire()
                        cond.notify()
                        cond.release()
                    if(node_id == 0):
                        cond.acquire()
                        while(show_all_nodes.qsize() != len(nodes)):
                            cond.wait()
                        cond.release()
                        while(show_all_nodes.qsize() != 0):
                            print(show_all_nodes.get()[1])
                            i=0
                else:
                    print(data)
         
            go = 1

def get_socket(port, finger_table):
    '''
    if finger_table:
        for i in range(8):
            if finger_table[i][0] == port-8000 and finger_table[i][1] != "self":
                return finger_table[i][1] #if the socket has already been made, return
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        #print "Connecting to Port", port
        s.connect(("127.0.0.1", int(port)))

        return s
    except:
        return False

def start_node(port):
    keys = [] #list of all keys in this node
    predecessor_keys = [] #list of all keys in the predecessor's node
    new_table = [0 for x in range(8)] #maps node_ids to sockets
    predecessor_id = 0
    count = 0

    if(port == 8000):
        keys = [x for x in range(256)]

    client = get_socket(8000 - 1, False) # get socket to client
    while not client:
        client = get_socket(8000 - 1, False)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe
    q = Queue.Queue()

    accept_thread = Thread(target = start_accepting, args= (s, q))
    #print "Created accept thread"
    accept_thread.daemon = True
    #print "started accept thread for ", port
    accept_thread.start()

    node_id = port - 8000

    while True:

        data = q.get()

        if(data == "crash"): #crash
            #CRASH THE NODE
            client.sendall("P" + str(node_id) + " Crashed\n")
            #print "hi"

        elif("show" in data): #show
            line = "N" + str(node_id) + "\n"
            temp = []
            for i in range(8):
                temp.append(finger_table[i][0])
            line +=  "FingerTable: " + str(temp) + "\n"
            line +=  "Keys: " + str(keys)
            if("all" in data):
                line += "\nall"
            client.sendall(line)

        elif(data.split()[0] == "find"): #find p k (i)
            key = int(data.split()[2])
            if key in keys:
                if(len(data.split()) == 3): # For a regular find operation
                    client.sendall(str(node_id) + "\n")
                else: # For a finger table initialization find function (using i)
                     sock = get_socket(8000, finger_table)
                     string = "combine " + data.split()[3] + " " + str(node_id) + " " + str(key) #sends i, i'th member of finger table, and key
                     sock.sendall(string + "\n")
            else:
                found = find(key, finger_table, node_id)
                found[1].sendall(data + '\n')

        elif(data.split()[0] == "finger"): #finger new_finger keys predecessor_id
            #print data
            predecessor_id = int(data.split()[3])
            new_finger = int(data.split()[1])
            #print "node: " + str(node_id) + " is being updated with " + data.split()[1]
            if(new_finger != node_id):
                finger_table = update_fingers(node_id, new_finger, finger_table)
            
            keys = set_keys(predecessor_id, node_id)

        elif(data.split()[0] == "keys"): #keys predecessor_id
            predecessor_id = int(data.split()[1])
            keys = set_keys(predecessor_id, node_id)

        elif(data.split()[0] == "predecessor"): #predecessor predecessor_id
            predecessor_id = int(data.split()[1])

        elif(data.split()[0] == "join"): #join
            #INIT Finger Table
            if(int(node_id) == 0):
                finger_table = [[0, "self"] for x in range(8)]
                client.sendall("P0 Joined\n")

            else:
                sock = get_socket(8000, False)
                sock.sendall("init " + str(node_id) + "\n")

        elif(data.split()[0] == "init"): #init p
            #print node_id
            #print data
            #print finger_table
            search_id = int(data.split()[1])
            for i in range(8):
                val = (search_id + 2**i) % 255
                string = "find " + str(finger_table[0][0]) + " " + str(val) + " " + str(i)
                sock = finger_table[0][1]
                if(sock == "self"):
                    sock = get_socket(8000, finger_table)
                sock.sendall(string + "\n")

        elif(data.split()[0] == "combine"): #initializing finger table, combine index i'th finger key
            #print data
            #print node_id
            i = int(data.split()[1])
            finger = data.split()[2]
            key = int(data.split()[3])
            new_table[i] = finger
            count += 1
            if count == 8:
                count = 0
                #print finger
                #print i
                port = 8000 + (key - 2**i) % 255
                #print port
                sock = get_socket(port, finger_table)
                sock.sendall(str(new_table) + "\n")
        else:
            finger_table = ast.literal_eval(data)
            #print finger_table
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
                        s = get_socket(int(finger_table[i]) + 8000, finger_table)
                    if not s:
                        print "Could not connect to " + str(finger_table[i])
                    
                    finger_table[i] = [int(finger_table[i]), s]
                    
                    if(successor): # Send the node_id to the successor to update the finger_table and the keys flag to update the keys
                        finger_table[i][1].sendall("finger " + str(node_id) + " keys " + str(node_id) + "\n")
                        successor = 0

                prev = finger_table[i][0]

            client.sendall("P" + str(node_id) + " Joined" + "\n")

def set_keys(predecessor_id, node_id):
    if(predecessor_id > node_id):
        keys = [x for x in range(0, node_id + 1)]
        keys += (x for x in range(predecessor_id + 1, 256))
    elif(predecessor_id < node_id):
        keys = [x for x in range(predecessor_id + 1, node_id + 1)]
    else:
        print "Setting Incorrect Keys" #Should never print

    return keys

def find(value, finger_table, node_id):
    successor = 1
    prev_node = -1
    sent = 0
    #for node in finger_table:
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
    #if finger[1] == "self" and finger[0] != node_id:
    #    finger[1] = get_socket(int(finger[0] + 8000))
    #print "node_id", node_id
    #print "value", value
    return finger

def start_accepting(s, q):
    while True:
        conn, addr = s.accept()
        read_thread = Thread(target = read_from_conns, args= (conn, q))
        #print "Created read thread"
        read_thread.daemon = True
        #print "started read thread for "
        read_thread.start()

def read_from_conns(conn, q):
    while True:
        data = conn.recv(1024)
        commands = data.split("\n")
        for command in commands:

            #print "Data:",data

            if(command == ""):
                continue

            else:
                #print "Data:",command[:1]
                q.put(command)

def update_fingers(node_id, new_finger, finger_table):
    #print "id",node_id
    #if new_finger > (node_id + 2**7)%255:
    #    return finger_table
    s = False
    successor = 1
    for i in range(8):
        if (new_finger < finger_table[i][0] or finger_table[i][0] == 0) and new_finger >= (node_id + 2 ** i)%255:
            #print "str", finger_table
            finger_table[i][0] = new_finger

            if not s:
                s = get_socket(new_finger + 8000, finger_table)
                if not s:
                    print "Could not connect to " + str(new_finger)

            finger_table[i][1] = s

        if(finger_table[i][0] != node_id and successor):
            if finger_table[len(finger_table)-1][0] == new_finger:
                finger_table[len(finger_table)-1][1].sendall("predecessor " + str(node_id) + "\n")
            elif new_finger != node_id:
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