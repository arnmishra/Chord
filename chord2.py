#!/usr/bin/python
import socket
import sys
import time
import random
import mutex
import pickle
import Queue as Q
import signal
from threading import Lock, Thread

def main(argv):

	min_delay, max_delay, port_0 = parse_file(argv)

	try:
		#client
		client_thread = Thread(target=create_client, args = ())
		client_thread.daemon = True
		client_thread.start()

	except:
		print("Unable to start client")

	while(True):
		time.sleep(10)

def parse_file(config_file):
	with open(config_file) as f:
		for line in f:
			data = line.split()
			if(len(data) == 2):
				min_delay = data[0]
				max_delay = data[1]
			if(len(data) == 1):
				port_0 = data[0]
	return min_delay, max_delay, port_0

def create_client(port_0):
	nodes = {}

	#Create client socket
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", int(port_0 - 10)))
    s.listen(33)

	#accept thread which accepts connections and then spawns new printing threads
	accept_thread = Thread(target = accept_client_connections, args = (s))
	accept_thread.daemon = True
	accept_thread.start()

	#Create node 0 thread
	start_node_0 = Thread(target = node_join, args = (port_0))
	start_node_0.daemon = True
	start_node_0.start()

	while True:
		command = raw_input('').split()
		if(command[0] == "join"):
			

def accept_client_connections(client_socket):
	while True:
		#each connection 
		conn, addr = s.accept()
		print_thread = Thread(target = print_from_connetions, args = (conn))
		print_thread.daemon = True
		print_thread.start()

def print_from_connetions(client_server_connection):
	while True:
		data = conn.recv(1024)
		print(data)

if __name__ == "__main__":
	main(sys.argv[1])
