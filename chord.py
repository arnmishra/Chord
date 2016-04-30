import collections
import sys
import socket
import time
import ast
import Queue
import pickle
import copy
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

    min_delay = parts[0][0]
    max_delay = parts[0][1]
    initial_port = parts[1]
    client_thread = Thread(target=client, args=(initial_port)) #start client
    #print "Created Client Thread"
    client_thread.daemon = True
    client_thread.start()
    global num_nodes
    num_nodes = 0
    '''
    global zero
    zero = False
    zero = get_socket(5000, False)
    while not zero:
        zero = get_socket(5000, False)
    '''
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
    print_thread = Thread(target = start_printing, args=([5000 - 1])) #start printing from the client
    #print "Created Start Print Thread"
    print_thread.daemon = True
    print_thread.start()

    port = int(initial_port) #Start Node 0 on  initial port
    node_thread = Thread(target=start_node, args=([port])) #start each node
    #print "Created Node Thread"
    node_thread.daemon = True
    node_thread.start()
    nodes[0] = get_socket(port, False)
    while (not nodes[0]):
        nodes[0] = get_socket(port, False) # Continue trying to connect till it connects
    #nodes['0'].sendall(get_fingers('0', nodes))
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
            #print(nodes.items())
            nodes = collections.OrderedDict(sorted(nodes.items())) #Sort the nodes in increasing order
            #print(nodes.items())
            #nodes[node_id].sendall(get_fingers(node_id, nodes))
            #print("sending join")
            nodes[node_id].sendall("join\n")
            
        elif command == "find": #find p k

            nodes[node_id].sendall(message + "\n") #send the key that is being searched for

        elif command == "crash": #crash p

            nodes[node_id].sendall("crash\n") #tell the node to crash
            del nodes[node_id] #remove the node from the list of connections

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
        #print "Created print thread"
        print_thread.daemon = True
        print_thread.start()

def print_from_conns(conn):
    global go, show_all, show_all_nodes
    show_all_nodes = Queue.PriorityQueue()
    while True:
        data = conn.recv(4096)
        node_id = data.split('\n')[0][1:].strip()
        if(len(data.split('\n')) == 7):
            contains_all = data.split('\n')[6].strip()
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
                    data = message[1] + '\n' + message[2] + '\n' + message[3] + '\n' + message[4] + '\n' + message[5]
                    print("========Node " + message[0] + "======== ")
                    print(data)
                    print("\n")
                    #print("here ddfd")
                    cond.acquire()
                    cond.notifyAll()
                    cond.release()
                else:
                    #i = 0
                    print(data)
         
            go = 1

def get_socket(port, finger_table):
    '''
    global zero
    if int(port) == 5000 and zero:
        return zero

    if finger_table:
        for i in range(8):
            if int(finger_table[i][0]) == int(port)-5000 and finger_table[i][1] != "self":
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
    global num_nodes
    keys = [] #list of all keys in this node
     #list of all keys in the predecessor's node
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
    #print(port)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe
    q = Queue.Queue()
    q2 = Queue.Queue()

    node_id = port - 5000

    accept_thread = Thread(target = start_accepting, args= (s, q, q2, node_id, conns))
    #print "Created accept thread"
    accept_thread.daemon = True
    #print "started accept thread for ", port
    accept_thread.start()

    while True:
        #print "at the top"
        #print(predecessor_keys)
        #print(hello)
        data = q.get()

        if(data.split()[0] == "predecessor"): #predecessor predecessor_id
            #print("setting pred keys for ", node_id)
            #print predecessor_keys
            predecessor_keys = []
            for i in range(2,len(data.split())):
                predecessor_keys.append(int(data.split()[i]))
            sock = get_socket(5000 + predecessor_id, finger_table)
            #print(predecessor_keys)
            sock.sendall("super_successor " + str(finger_table[0][0]) + '\n')

        elif(data.split()[0] == "super_successor"): #super_successor id
            super_successor = data.split()[1]

        elif(data == "crash"): #crash
            #CRASH THE NODE
            #found = find(node_id - 1, finger_table, node_id)
            #print(found[0], found[1])
            for finger in finger_table:
                try:
                    finger[1].shutdown(socket.SHUT_RDWR)
                    finger[1].close()
                except:
                    i = 0
            for conn in conns:
               conn.close()
            client.sendall("P" + str(node_id) + " Crashed\n")
            #print "hi"

        elif("show" in data): #show
            #print("already set keys")
            #print(predecessor_keys)
            line = "N" + str(node_id) + "\n"
            temp = []
            for i in range(8):
                temp.append(finger_table[i][0])
            line +=  "FingerTable: " + str(temp) + "\n"
            line +=  "Keys: " + str(keys) + "\n"
            #print(predecessor_keys)
            line += "predecessor_keys: " + str(predecessor_keys) + "\n"
            line += "super_successor: " + str(super_successor) + "\n"
            line += "predecessor_id: " + str(predecessor_id)
            if("all" in data):
                line += "\nall"
            #print("sent show from " + str(node_id))
            client.sendall(line)

        elif(data.split()[0] == "find"): #find p k (i)
            key = int(data.split()[2])
            if key in keys:
                if(len(data.split()) == 3): # For a regular find operation
                    client.sendall(str(node_id) + "\n")
                else: # For a finger table initialization find function (using i)
                     sock = get_socket(5000, finger_table)
                     string = "combine " + data.split()[3] + " " + str(node_id) + " " + str(key) #sends i, i'th member of finger table, and key
                     sock.sendall(string + "\n")
            else:
                found = find(key, finger_table, node_id)
                found[1].sendall(data + '\n')

        elif(data.split()[0] == "finger"): #finger new_finger keys predecessor_id
            #print node_id
            #print data
            predecessor_id = int(data.split()[3])
            new_finger = int(data.split()[1])
            #print "node: " + str(node_id) + " is being updated with " + data.split()[1]
            if(new_finger != node_id):
                finger_table = update_fingers(node_id, new_finger, finger_table)
                q2.put(finger_table[0][0])
            #if(finger_table[0][0] == new_finger):
            #send_keys = ""
            #for i in range(len(keys)):
            #    send_keys += str(keys[i]) + " "
                #send_keys += "]"
                #print(send_keys)
            #finger_table[0][1].sendall("predecessor " + str(node_id) + " " + send_keys)
            keys = set_keys(finger_table, predecessor_id, node_id)

        elif(data.split()[0] == "keys"): #keys predecessor_id
            predecessor_id = int(data.split()[1])
            keys = set_keys(finger_table, predecessor_id, node_id)

        elif(data.split()[0] == "join"): #join
            #INIT Finger Table
            if(int(node_id) == 0):
                nodes_lock.acquire()
                num_nodes += 1
                nodes_lock.release()
                finger_table = [[0, "self"] for x in range(8)]
                client.sendall("P0 Joined\n")
                heartbeat_thread = Thread(target=send_heartbeats, args=(finger_table[0], node_id))
                heartbeat_thread.daemon = True
                heartbeat_thread.start()

            else:
                sock = get_socket(5000, False)
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
                    sock = get_socket(5000, finger_table)
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
                port = 5000 + (key - 2**i) % 255
                #print port
                sock = get_socket(port, finger_table)
                sock.sendall(str(new_table) + "\n")

        elif(data == "change successor"):
            #print finger_table[0]
            old_id = finger_table[0][0]
            sock = get_socket(int(super_successor) + 5000, finger_table)
            for finger in finger_table:
                if finger[0] == old_id:
                    finger[0] = int(super_successor)
                    finger[1].close()
                    finger[1] = sock
            send_keys = ""
            for i in range(len(keys)):
                send_keys += str(keys[i]) + " "
            finger_table[0][1].sendall("concatenate " + str(node_id) + " " + str(old_id) + " " + send_keys)
            sock = get_socket(predecessor_id + 5000, finger_table)
            sock.sendall("super_successor " + str(finger_table[0][0]))


        elif(data.split()[0] == "concatenate"):
            keys += predecessor_keys
            keys.sort()
            predecessor_id = int(data.split()[1])
            old_id = int(data.split()[2])
            for finger in finger_table:
                if finger[0] == old_id:
                    finger[0] = int(super_successor)
                    finger[1].close()
                    finger[1] = sock
            predecessor_keys = []
            for i in range(3,len(data.split())):
                predecessor_keys.append(int(data.split()[i]))
            sock = get_socket(predecessor_id + 5000, finger_table)
            sock.sendall("super_successor " + str(finger_table[0][0]))
            send_keys = ""
            for i in range(len(keys)):
                send_keys += str(keys[i]) + " "
            finger_table[0][1].sendall("predecessor " + str(predecessor_id) + " " + send_keys)


        else:
            #rint data
            predecessor_keys = []
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
            client.sendall("P" + str(node_id) + " Joined" + "\n")
            #Start heartbeat messages
            heartbeat_thread = Thread(target=send_heartbeats, args=(finger_table[0], node_id))
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
        #print("at the bottom")
        #print(predecessor_keys)
        #print(hello)

#Send heartbeats to successor
def send_heartbeats(successor, node_id):
    if(num_nodes <= 1):
        with cond2:
            cond2.wait()
    successor[1].settimeout(5)
    time_t = str(time.time())
    time.sleep(2.5)
    successor[1].sendall("heartbeat message start " + time_t + " " + str(node_id))
    while True:
        #print("Node joined - heartbeating started")
        #try:
        data = successor[1].recv(1024)
        if not data:
            print("N" + str(node_id) + "'s Successor Crashed")
            me = get_socket(5000 + node_id, False)
            me.sendall("change successor\n")
            return
        #print("NODE CRASHED 1")
        #return
        if("end heartbeat" in data):
            #print "enter"
            #print(str(node_id) + " finished heartbeat to " + str(finger_table[0][0]))
            #STRING IS 0 -> end 1-> heartbeat 2-> time initially sent from predecssor 3-> time received by successor
            time_sent_precessor = float(data.split()[2])
            time_sent_successor = float(data.split()[3])
            #print(int(time_sent_successor - time_sent_precessor))
            #if(int(time_sent_successor - time_sent_precessor) == 5):
            time_t = str(time.time())
            time.sleep(2.5)
            try:
                successor[1].sendall("heartbeat message start " + time_t + " " + str(node_id))
            except:
                return
        '''except socket.timeout, e:
            err = e.args[0]
            if(err == 'timed out'):
                print("NODE CRASHED 2")
                #DO A LOTTA SHIT
                i = 0'''

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
    finger_table[0][1].sendall("predecessor " + str(node_id) + " " + send_keys)
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
    #    finger[1] = get_socket(int(finger[0] + 5000))
    #print "node_id", node_id
    #print "value", value
    return finger

def start_accepting(s, q, q2, node_id, conns):
    while True:
        conn, addr = s.accept()
        conns.append(conn)
        read_thread = Thread(target = read_from_conns, args= (conn, q, q2, node_id))
        #print "Created read thread"
        read_thread.daemon = True
        #print "started read thread for "
        read_thread.start()

def read_from_conns(conn, q, q2, node_id):
    while True:
        #Goes into except if node crashes and connection is closed
        try:
            data = conn.recv(4096)
        except:
            #print("NODE CRASHED - " + str(node_id))
            return
        if("heartbeat" in data):
            i = 0
            if("start" in data):
                #print(str(node_id) + " received heartbeat from " + data.split()[4])
                time.sleep(2.5)
                time_received = data.split()[3]
                #print(str(node_id) + " sent heartbeat back to " + data.split()[4])
                try:
                    successor = -1
                    #print q2.empty()
                    while not q2.empty():
                        #print "pull off queue"
                        successor = q2.get()
                    conn.sendall("end heartbeat " + str(time_received) + " " + str(time.time()) + " " + str(successor))
                    #print("here")
                except:
                    return
            continue
        else:
            commands = data.split("\n")
            for command in commands:

                if(command == ""):
                    continue

                else:
                    #print "Data:",command
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
                s = get_socket(new_finger + 5000, finger_table)
                if not s:
                    print "Could not connect to " + str(new_finger)

            finger_table[i][1] = s

        if(finger_table[i][0] != node_id and successor):
            #if finger_table[len(finger_table)-1][0] == new_finger:
                #finger_table[len(finger_table)-1][1].sendall("predecessor " + str(node_id) + "\n")
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